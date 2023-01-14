package de.invesdwin.context.integration.channel.sync.mina.apr;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiTomcatNativeSocketChannelTest extends AChannelTest {

    @Test
    public void testTomcatSocketChannelPerformance() throws InterruptedException {
        if (OperatingSystem.isWindows()) {
            //not supported on windows
            return;
        }
        Throwables.setDebugStackTraceEnabled(true);
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runTomcatSocketChannelPerformanceTest(address);
    }

    private void runTomcatSocketChannelPerformanceTest(final InetSocketAddress address) throws InterruptedException {
        final TomcatNativeSocketSynchronousChannel serverChannel = new TomcatNativeSocketSynchronousChannel(address,
                true, getMaxMessageSize());
        final TomcatNativeSocketSynchronousChannel clientChannel = new TomcatNativeSocketSynchronousChannel(address,
                false, getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new TomcatNativeSocketSynchronousWriter(
                serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new TomcatNativeSocketSynchronousReader(
                serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runTomcatSocketChannelPerformanceTest",
                1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new TomcatNativeSocketSynchronousWriter(
                clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new TomcatNativeSocketSynchronousReader(
                clientChannel);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
