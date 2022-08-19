package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.DisabledHandshakeProvider;
import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.EpollNettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class BidiNettySocketChannelTest extends AChannelTest {

    @Test
    public void testBidiNettySocketChannelPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runBidiNettySocketChannelPerformanceTest(EpollNettySocketChannelType.INSTANCE, address);
    }

    private void runBidiNettySocketChannelPerformanceTest(final INettySocketChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final NettySocketChannel serverChannel = newNettySocketChannel(type, address, true, getMaxMessageSize());
        final NettySocketChannel clientChannel = newNettySocketChannel(type, address, false, getMaxMessageSize());

        //just synchronize open/close of readers and writers since they happen in different threads
        final HandshakeChannelFactory handshake = new HandshakeChannelFactory(
                new DisabledHandshakeProvider(MAX_WAIT_DURATION));

        final ISynchronousWriter<IByteBufferWriter> responseWriter = handshake
                .newWriter(newNettySocketSynchronousWriter(serverChannel));
        final ISynchronousReader<IByteBuffer> requestReader = handshake
                .newReader(newNettySocketSynchronousReader(serverChannel));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNettySocketChannelPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = handshake
                .newWriter(newNettySocketSynchronousWriter(clientChannel));
        final ISynchronousReader<IByteBuffer> responseReader = handshake
                .newReader(newNettySocketSynchronousReader(clientChannel));
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    protected ISynchronousReader<IByteBuffer> newNettySocketSynchronousReader(final NettySocketChannel serverChannel) {
        return new NettySocketSynchronousReader(serverChannel);
    }

    protected ISynchronousWriter<IByteBufferWriter> newNettySocketSynchronousWriter(
            final NettySocketChannel serverChannel) {
        return new NettySocketSynchronousWriter(serverChannel);
    }

    protected NettySocketChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettySocketChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
