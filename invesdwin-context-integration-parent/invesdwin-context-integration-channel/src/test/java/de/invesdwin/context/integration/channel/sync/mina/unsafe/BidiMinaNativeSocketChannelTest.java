package de.invesdwin.context.integration.channel.sync.mina.unsafe;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.context.integration.channel.sync.mina.type.MinaSocketType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiMinaNativeSocketChannelTest extends AChannelTest {

    @Test
    public void testMinaSocketChannelPerformance() throws InterruptedException {
        if (OperatingSystem.isWindows()) {
            //not supported on windows
            return;
        }
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runMinaSocketChannelPerformanceTest(MinaSocketType.AprTcp, address);
    }

    private void runMinaSocketChannelPerformanceTest(final IMinaSocketType type, final InetSocketAddress address)
            throws InterruptedException {
        final MinaSocketSynchronousChannel serverChannel = new MinaSocketSynchronousChannel(type, address, true,
                getMaxMessageSize());
        final MinaSocketSynchronousChannel clientChannel = new MinaSocketSynchronousChannel(type, address, false,
                getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new MinaNativeSocketSynchronousWriter(
                serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new MinaNativeSocketSynchronousReader(
                serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runMinaSocketChannelPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new MinaNativeSocketSynchronousWriter(
                clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new MinaNativeSocketSynchronousReader(
                clientChannel);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
