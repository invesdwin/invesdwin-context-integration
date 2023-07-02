package de.invesdwin.context.integration.channel.sync.netty.udp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;

@Immutable
public class NioNettyDatagramChannelType implements INettyDatagramChannelType {

    public static final NioNettyDatagramChannelType INSTANCE = new NioNettyDatagramChannelType();

    @Override
    public EventLoopGroup newServerWorkerGroup(final int threadCount,
            final SelectStrategyFactory selectStrategyFactory) {
        return new NioEventLoopGroup(threadCount);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup(final int threadCount,
            final SelectStrategyFactory selectStrategyFactory) {
        return new NioEventLoopGroup(threadCount);
    }

    @Override
    public Class<? extends DatagramChannel> getServerChannelType() {
        return NioDatagramChannel.class;
    }

    @Override
    public Class<? extends DatagramChannel> getClientChannelType() {
        return NioDatagramChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize, final boolean server) {
        consumer.option(ChannelOption.IP_TOS, BlockingDatagramSynchronousChannel.IPTOS_LOWDELAY
                | BlockingDatagramSynchronousChannel.IPTOS_THROUGHPUT);
        consumer.option(ChannelOption.SO_SNDBUF, socketSize);
        consumer.option(ChannelOption.SO_RCVBUF, ByteBuffers
                .calculateExpansion(socketSize * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER));
    }

    @Override
    public void initChannel(final DatagramChannel channel, final boolean server) {
        //noop
    }
}
