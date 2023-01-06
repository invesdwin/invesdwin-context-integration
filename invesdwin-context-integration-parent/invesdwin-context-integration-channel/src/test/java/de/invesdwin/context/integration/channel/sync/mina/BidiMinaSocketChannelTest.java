package de.invesdwin.context.integration.channel.sync.mina;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.transport.vmpipe.VmPipeAddress;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.context.integration.channel.sync.mina.type.MinaSocketType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiMinaSocketChannelTest extends AChannelTest {

    @Test
    public void testBidiMinaSocketChannelPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runBidiMinaSocketChannelPerformanceTest(MinaSocketType.NioTcp, address);
    }

    @Test
    public void testBidiMinaVmPipeChannelPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final VmPipeAddress address = new VmPipeAddress(port);
        runBidiMinaSocketChannelPerformanceTest(MinaSocketType.VmPipe, address);
    }

    private void runBidiMinaSocketChannelPerformanceTest(final IMinaSocketType type, final SocketAddress address)
            throws InterruptedException {
        final MinaSocketSynchronousChannel serverChannel = newMinaSocketChannel(type, address, true,
                getMaxMessageSize());
        final MinaSocketSynchronousChannel clientChannel = newMinaSocketChannel(type, address, false,
                getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new MinaSocketSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new MinaSocketSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runMinaSocketChannelPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new MinaSocketSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new MinaSocketSynchronousReader(clientChannel);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    protected MinaSocketSynchronousChannel newMinaSocketChannel(final IMinaSocketType type,
            final SocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new MinaSocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
