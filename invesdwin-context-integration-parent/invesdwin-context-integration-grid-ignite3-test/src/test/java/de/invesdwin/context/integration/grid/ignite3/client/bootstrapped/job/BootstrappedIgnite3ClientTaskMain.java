package de.invesdwin.context.integration.grid.ignite3.client.bootstrapped.job;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

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
import de.invesdwin.context.log.Log;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.BroadcastingOutputStream;
import de.invesdwin.util.streams.log.LogLevelOutputStream;
import de.invesdwin.util.time.date.FDate;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

@NotThreadSafe
public class BootstrappedIgnite3ClientTaskMain extends AMain {

    private static final boolean BOOTSTRAP = true;

    static {
        AspectJWeaverIncludesConfigurer.setShowWeaveInfo(false);
        PlatformInitializerProperties.setAllowed(BOOTSTRAP);
    }

    @Option(name = "-s", aliases = "--size", usage = "Defines the number of processes", required = true)
    protected int size;
    @Option(name = "-r", aliases = "--rank", usage = "Defines the rank of this process", required = true)
    protected int rank;
    @Option(name = "-a", aliases = "--serverAddress", usage = "The server address host:port", required = true)
    protected String serverAddressStr;
    @Option(name = "-t", aliases = "--tempFile", usage = "Temp file to write results to", required = true)
    protected String tempFile;

    public BootstrappedIgnite3ClientTaskMain(final String[] args) {
        super(args, BOOTSTRAP);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        final InlineChannelTest parent = new InlineChannelTest();
        final FastByteArrayOutputStream memoryLogStream = new FastByteArrayOutputStream();

        final String[] split = Strings.splitPreserveAllTokens(serverAddressStr, ":");
        final InetSocketAddress serverAddress = new InetSocketAddress(split[0], Integer.parseInt(split[1]));

        try {
            if (rank == 0) {
                final SocketSynchronousChannel serverChannel = newSocketSynchronousChannel(serverAddress, true,
                        parent.getMaxMessageSize());
                final ISynchronousReader<FDate> requestReader = parent
                        .newSerdeReader(new NativeSocketSynchronousReader(serverChannel));
                final ISynchronousWriter<FDate> responseWriter = parent
                        .newSerdeWriter(new NativeSocketSynchronousWriter(serverChannel));

                try (OutputStream log = newInMemoryLog(LatencyServerTask.class, memoryLogStream)) {
                    new LatencyServerTask(parent, log, requestReader, responseWriter).run();
                }
            } else if (rank == 1) {
                final SocketSynchronousChannel clientChannel = newSocketSynchronousChannel(serverAddress, false,
                        parent.getMaxMessageSize());
                final ISynchronousWriter<FDate> requestWriter = parent
                        .newSerdeWriter(new NativeSocketSynchronousWriter(clientChannel));
                final ISynchronousReader<FDate> responseReader = parent
                        .newSerdeReader(new NativeSocketSynchronousReader(clientChannel));

                try (OutputStream log = newInMemoryLog(LatencyClientTask.class, memoryLogStream)) {
                    new LatencyClientTask(parent, log, requestWriter, responseReader).run();
                }
            } else {
                throw UnknownArgumentException.newInstance(int.class, rank);
            }

            // Write the captured in-memory log stream to the specified temp file for the parent task
            try (FileOutputStream fos = new FileOutputStream(new File(tempFile))) {
                memoryLogStream.writeTo(fos);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private OutputStream newInMemoryLog(final Class<?> taskClass, final FastByteArrayOutputStream memoryLogStream) {
        final LogLevelOutputStream consoleLog = new LogLevelOutputStream(LogLevel.INFO, new Log(taskClass));
        return new BroadcastingOutputStream(consoleLog, memoryLogStream);
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, true);
    }

    public static void main(final String[] args) {
        new BootstrappedIgnite3ClientTaskMain(args).run();
    }
}