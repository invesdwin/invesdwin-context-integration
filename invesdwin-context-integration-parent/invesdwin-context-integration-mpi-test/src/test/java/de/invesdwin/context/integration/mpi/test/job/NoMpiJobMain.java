package de.invesdwin.context.integration.mpi.test.job;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.PlatformInitializerProperties;
import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.beans.init.platform.util.AspectJWeaverIncludesConfigurer;
import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.BroadcastingOutputStream;
import de.invesdwin.util.streams.log.LogLevelOutputStream;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;

/**
 * This job works without dependending on any MPI implementation, it just requires some env vars passed as arguments and
 * a shared file system to share the server address between the client and server. Such jobs can be used with any MPI
 * implementation regardless if they provide a java library or not.
 */
@NotThreadSafe
public class NoMpiJobMain extends AMain {

    private static final boolean BOOTSTRAP = true;

    static {
        AspectJWeaverIncludesConfigurer.setShowWeaveInfo(false);
        PlatformInitializerProperties.setAllowed(BOOTSTRAP);
    }

    @Option(help = true, name = "-l", aliases = "--logDir", usage = "Defines the log directory")
    protected File logDir;
    @Option(help = true, name = "-s", aliases = "--size", usage = "Defines the number of processes")
    protected int size;
    @Option(help = true, name = "-r", aliases = "--rank", usage = "Defines the rank of this process")
    protected int rank;

    public NoMpiJobMain(final String[] args) {
        super(args, BOOTSTRAP);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        test();
    }

    private void test() {
        Assertions.assertThat(rank).isBetween(0, 1);
        testPerformance();
    }

    private void testPerformance() {
        final AChannelTest parent = new AChannelTest() {
        };
        //logDir should be shared between all processes, e.g. a shared file system
        final File serverAddressFile = new File(logDir, "serverAddress.txt");
        switch (rank) {
        case 0: {
            final InetSocketAddress serverAddress = waitForServerAddress(serverAddressFile);
            final SocketSynchronousChannel clientChannel = newSocketSynchronousChannel(serverAddress, false,
                    parent.getMaxMessageSize());
            final ISynchronousWriter<FDate> requestWriter = parent
                    .newSerdeWriter(new NativeSocketSynchronousWriter(clientChannel));
            final ISynchronousReader<FDate> responseReader = parent
                    .newSerdeReader(new NativeSocketSynchronousReader(clientChannel));
            try (OutputStream log = newLog(rank, size, LatencyClientTask.class)) {
                new LatencyClientTask(parent, log, requestWriter, responseReader).run();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            break;
        }
        case 1: {
            final String serverHostname = NetworkUtil.getHostname();
            final int serverPort = NetworkUtil.findAvailableTcpPort();
            final InetSocketAddress serverAddress = new InetSocketAddress(serverHostname, serverPort);
            final String serverAddressStr = serverHostname + ":" + serverPort;
            try {
                Files.writeStringToFile(serverAddressFile, serverAddressStr, Charset.defaultCharset());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            try {
                final SocketSynchronousChannel serverChannel = newSocketSynchronousChannel(serverAddress, true,
                        parent.getMaxMessageSize());
                final ISynchronousReader<FDate> requestReader = parent
                        .newSerdeReader(new NativeSocketSynchronousReader(serverChannel));
                final ISynchronousWriter<FDate> responseWriter = parent
                        .newSerdeWriter(new NativeSocketSynchronousWriter(serverChannel));
                try (OutputStream log = newLog(rank, size, LatencyServerTask.class)) {
                    new LatencyServerTask(parent, log, requestReader, responseWriter).run();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                Files.deleteQuietly(serverAddressFile);
            }
            break;
        }
        default:
            throw UnknownArgumentException.newInstance(int.class, rank);
        }
    }

    private InetSocketAddress waitForServerAddress(final File serverAddressFile) {
        final Instant start = new Instant();
        while (!serverAddressFile.exists()) {
            FTimeUnit.MILLISECONDS.sleepNoInterrupt(1);
            if (start.isGreaterThan(ContextProperties.DEFAULT_NETWORK_TIMEOUT)) {
                throw new RuntimeException("Timeout waiting for server address file");
            }
        }
        final String serverAddressStr = Files.readFileToStringNoThrow(serverAddressFile, Charset.defaultCharset());
        final String[] serverAddressStrSplit = Strings.splitPreserveAllTokens(serverAddressStr, ":");
        final String serverHostname = serverAddressStrSplit[0];
        final int serverPort = Integer.parseInt(serverAddressStrSplit[1]);
        return new InetSocketAddress(serverHostname, serverPort);
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, true);
    }

    private OutputStream newLog(final int rank, final int size, final Class<?> taskClass) throws FileNotFoundException {
        final LogLevelOutputStream log = new LogLevelOutputStream(LogLevel.INFO, new Log(taskClass));
        if (logDir == null) {
            return log;
        }
        final BufferedOutputStream file = new BufferedOutputStream(new FileOutputStream(
                new File(logDir, (rank + 1) + "_" + size + "_" + taskClass.getSimpleName() + ".log")));
        return new BroadcastingOutputStream(log, file);
    }

    public static void main(final String[] args) {
        try {
            new NoMpiJobMain(args).run();
        } catch (final Throwable t) {
            Err.process(t);
        }
        //kill any outstanding threads
        System.exit(0);
    }

}
