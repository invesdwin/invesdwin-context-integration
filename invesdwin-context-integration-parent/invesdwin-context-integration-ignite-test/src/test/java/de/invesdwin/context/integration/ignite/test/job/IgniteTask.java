package de.invesdwin.context.integration.ignite.test.job;

import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteCallable;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.InlineChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.log.LogLevel;
import de.invesdwin.util.streams.BroadcastingOutputStream;
import de.invesdwin.util.streams.log.LogLevelOutputStream;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

@NotThreadSafe
public class IgniteTask implements IgniteCallable<IgniteTask.TaskResult> {

    public static final String JOB_STATE_CACHE = "igniteJobStateCache";
    public static final String KEY_SERVER_ADDRESS = "serverAddress";

    private final int rank;
    private final int size;

    public IgniteTask(final int rank, final int size) {
        this.rank = rank;
        this.size = size;
    }

    @Override
    public TaskResult call() throws Exception {
        final Ignite ignite = Ignition.ignite();
        final IgniteCache<String, String> cache = ignite.getOrCreateCache(JOB_STATE_CACHE);
        final InlineChannelTest parent = new InlineChannelTest();

        final FastByteArrayOutputStream memoryLogStream = new FastByteArrayOutputStream();
        final Class<?> taskClass;

        switch (rank) {
        case 0: {
            taskClass = LatencyServerTask.class;
            final String serverHostname = NetworkUtil.getLocalAddress().getHostAddress();
            final int serverPort = NetworkUtil.findAvailableTcpPort();
            final InetSocketAddress serverAddress = new InetSocketAddress(serverHostname, serverPort);

            // Share server address in memory via IgniteCache
            cache.put(KEY_SERVER_ADDRESS, serverHostname + ":" + serverPort);

            try {
                final SocketSynchronousChannel serverChannel = newSocketSynchronousChannel(serverAddress, true,
                        parent.getMaxMessageSize());
                final ISynchronousReader<FDate> requestReader = parent
                        .newSerdeReader(new NativeSocketSynchronousReader(serverChannel));
                final ISynchronousWriter<FDate> responseWriter = parent
                        .newSerdeWriter(new NativeSocketSynchronousWriter(serverChannel));

                try (OutputStream log = newInMemoryLog(taskClass, memoryLogStream)) {
                    new LatencyServerTask(parent, log, requestReader, responseWriter).run();
                }
            } finally {
                cache.remove(KEY_SERVER_ADDRESS);
            }
            break;
        }
        case 1: {
            taskClass = LatencyClientTask.class;
            final InetSocketAddress serverAddress = waitForServerAddress(cache);

            final SocketSynchronousChannel clientChannel = newSocketSynchronousChannel(serverAddress, false,
                    parent.getMaxMessageSize());
            final ISynchronousWriter<FDate> requestWriter = parent
                    .newSerdeWriter(new NativeSocketSynchronousWriter(clientChannel));
            final ISynchronousReader<FDate> responseReader = parent
                    .newSerdeReader(new NativeSocketSynchronousReader(clientChannel));

            try (OutputStream log = newInMemoryLog(taskClass, memoryLogStream)) {
                new LatencyClientTask(parent, log, requestWriter, responseReader).run();
            }
            break;
        }
        default:
            throw UnknownArgumentException.newInstance(int.class, rank);
        }

        final String logFileName = (rank + 1) + "_" + size + "_" + taskClass.getSimpleName() + ".log";
        final String logContent = memoryLogStream.toString(StandardCharsets.UTF_8);

        return new TaskResult(logFileName, logContent);
    }

    private InetSocketAddress waitForServerAddress(final IgniteCache<String, String> cache) {
        final Instant start = new Instant();
        while (!cache.containsKey(KEY_SERVER_ADDRESS)) {
            FTimeUnit.MILLISECONDS.sleepNoInterrupt(50);
            if (start.isGreaterThan(ContextProperties.DEFAULT_NETWORK_TIMEOUT)) {
                throw new RuntimeException("Timeout waiting for server address in IgniteCache");
            }
        }

        final String serverAddressStr = cache.get(KEY_SERVER_ADDRESS);
        final String[] split = Strings.splitPreserveAllTokens(serverAddressStr, ":");
        return new InetSocketAddress(split[0], Integer.parseInt(split[1]));
    }

    private OutputStream newInMemoryLog(final Class<?> taskClass, final FastByteArrayOutputStream memoryLogStream) {
        final LogLevelOutputStream consoleLog = new LogLevelOutputStream(LogLevel.INFO, new Log(taskClass));
        return new BroadcastingOutputStream(consoleLog, memoryLogStream);
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, true);
    }

    public static class TaskResult implements Serializable {
        private final String logFileName;
        private final String logContent;

        public TaskResult(final String logFileName, final String logContent) {
            this.logFileName = logFileName;
            this.logContent = logContent;
        }

        public String getLogFileName() {
            return logFileName;
        }

        public String getLogContent() {
            return logContent;
        }
    }
}