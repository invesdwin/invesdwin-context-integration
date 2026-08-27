package de.invesdwin.context.integration.grid.hadoop.test.yarn.job;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.PlatformInitializerProperties;
import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.beans.init.platform.util.AspectJWeaverIncludesConfigurer;
import de.invesdwin.context.integration.channel.InlineChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousWriter;
import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.BroadcastingOutputStream;
import de.invesdwin.util.streams.log.LogLevelOutputStream;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;

@NotThreadSafe
public class YarnJobMain extends AMain {

    private static final boolean BOOTSTRAP = true;

    static {
        AspectJWeaverIncludesConfigurer.setShowWeaveInfo(false);
        PlatformInitializerProperties.setAllowed(BOOTSTRAP);
    }

    @Option(name = "-l", aliases = "--logDir", usage = "Defines the log directory", required = true)
    protected String logDir;
    @Option(name = "-s", aliases = "--size", usage = "Defines the number of processes", required = true)
    protected int size;
    @Option(name = "-r", aliases = "--rank", usage = "Defines the rank of this process", required = true)
    protected int rank;

    public YarnJobMain(final String[] args) {
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
        final InlineChannelTest parent = new InlineChannelTest();

        final IFileChannel logChannel = FileChannelRegistry.newInstance(logDir);
        final IFileChannel serverAddressChannel = logChannel.withFilename("serverAddress.txt");

        switch (rank) {
        case 0: {
            final String serverHostname = NetworkUtil.getLocalAddress().getHostAddress();
            final int serverPort = NetworkUtil.findAvailableTcpPort();
            final InetSocketAddress serverAddress = new InetSocketAddress(serverHostname, serverPort);
            final String serverAddressStr = serverHostname + ":" + serverPort;

            try {
                // Write Address to HDFS
                serverAddressChannel.uploadString(serverAddressStr);
            } catch (final Exception e) {
                throw new RuntimeException("Failed to write server address to HDFS", e);
            }

            try {
                final SocketSynchronousChannel serverChannel = newSocketSynchronousChannel(serverAddress, true,
                        parent.getMaxMessageSize());
                final ISynchronousReader<FDate> requestReader = parent
                        .newSerdeReader(new NativeSocketSynchronousReader(serverChannel));
                final ISynchronousWriter<FDate> responseWriter = parent
                        .newSerdeWriter(new NativeSocketSynchronousWriter(serverChannel));
                try (OutputStream log = newLog(logChannel, LatencyServerTask.class)) {
                    new LatencyServerTask(parent, log, requestReader, responseWriter).run();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                // Clean up HDFS file after server shuts down
                try {
                    serverAddressChannel.delete();
                } catch (final Exception e) {
                    // Ignore or log cleanup failure
                }
            }
            break;
        }
        case 1: {
            // Pass the server address channel for waiting
            final InetSocketAddress serverAddress = waitForServerAddress(serverAddressChannel);

            final SocketSynchronousChannel clientChannel = newSocketSynchronousChannel(serverAddress, false,
                    parent.getMaxMessageSize());
            final ISynchronousWriter<FDate> requestWriter = parent
                    .newSerdeWriter(new NativeSocketSynchronousWriter(clientChannel));
            final ISynchronousReader<FDate> responseReader = parent
                    .newSerdeReader(new NativeSocketSynchronousReader(clientChannel));
            try (OutputStream log = newLog(logChannel, LatencyClientTask.class)) {
                new LatencyClientTask(parent, log, requestWriter, responseReader).run();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            break;
        }
        default:
            throw UnknownArgumentException.newInstance(int.class, rank);
        }
    }

    private InetSocketAddress waitForServerAddress(final IFileChannel serverAddressChannel) {
        final Instant start = new Instant();
        try {
            while (!serverAddressChannel.exists()) {
                FTimeUnit.MILLISECONDS.sleepNoInterrupt(500);

                if (start.isGreaterThan(ContextProperties.DEFAULT_NETWORK_TIMEOUT)) {
                    throw new RuntimeException("Timeout waiting for server address file in HDFS");
                }
            }

            final String serverAddressStr = serverAddressChannel.downloadString();
            final String[] serverAddressStrSplit = Strings.splitPreserveAllTokens(serverAddressStr, ":");
            final String serverHostname = serverAddressStrSplit[0];
            final int serverPort = Integer.parseInt(serverAddressStrSplit[1]);

            return new InetSocketAddress(serverHostname, serverPort);

        } catch (final Exception e) {
            throw new RuntimeException("Error reading server address from HDFS", e);
        }
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, true);
    }

    private OutputStream newLog(final IFileChannel logChannel, final Class<?> taskClass) {
        final LogLevelOutputStream log = new LogLevelOutputStream(LogLevel.INFO, new Log(taskClass));
        if (logDir == null) {
            return log;
        }

        final String logFileName = (rank + 1) + "_" + size + "_" + taskClass.getSimpleName() + ".log";
        final IFileChannel taskLogChannel = logChannel.withFilename(logFileName);
        final OutputStream hdfsOut = taskLogChannel.newUpload();

        return new BroadcastingOutputStream(log, hdfsOut);
    }

    public static void main(final String[] args) {
        new YarnJobMain(args).run();
    }

}