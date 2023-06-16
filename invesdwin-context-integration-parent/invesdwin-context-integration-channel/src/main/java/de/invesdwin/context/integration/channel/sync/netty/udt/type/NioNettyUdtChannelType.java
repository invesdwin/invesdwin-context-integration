package de.invesdwin.context.integration.channel.sync.netty.udt.type;

import java.util.concurrent.Executor;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.udt.UdtChannel;
import io.netty.channel.udt.UdtServerChannel;
import io.netty.channel.udt.nio.NioUdtProvider;

@Immutable
public class NioNettyUdtChannelType implements INettyUdtChannelType {

    public static final NioNettyUdtChannelType INSTANCE = new NioNettyUdtChannelType();

    @Override
    public EventLoopGroup newServerAcceptorGroup(final int threadCount) {
        return new NioEventLoopGroup(threadCount, (Executor) null, NioUdtProvider.MESSAGE_PROVIDER);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final int threadCount, final EventLoopGroup parentGroup) {
        return new NioEventLoopGroup(threadCount, (Executor) null, NioUdtProvider.MESSAGE_PROVIDER);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup(final int threadCount) {
        return new NioEventLoopGroup(threadCount, (Executor) null, NioUdtProvider.MESSAGE_PROVIDER);
    }

    @Override
    public ChannelFactory<? extends UdtServerChannel> getServerChannelFactory() {
        return NioUdtProvider.MESSAGE_ACCEPTOR;
    }

    @Override
    public ChannelFactory<? extends UdtChannel> getClientChannelFactory() {
        return NioUdtProvider.MESSAGE_CONNECTOR;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize, final boolean server) {
        consumer.option(ChannelOption.SO_SNDBUF, socketSize);
        consumer.option(ChannelOption.SO_RCVBUF, ByteBuffers
                .calculateExpansion(socketSize * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER));
    }

    @Override
    public void initChannel(final UdtChannel channel, final boolean server) throws Exception {
        //noop
    }
}
