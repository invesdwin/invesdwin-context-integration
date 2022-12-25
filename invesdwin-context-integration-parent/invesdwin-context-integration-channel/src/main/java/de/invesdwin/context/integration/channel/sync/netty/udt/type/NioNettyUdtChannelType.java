package de.invesdwin.context.integration.channel.sync.netty.udt.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.udt.UdtChannel;
import io.netty.channel.udt.UdtServerChannel;
import io.netty.channel.udt.nio.NioUdtMessageAcceptorChannel;
import io.netty.channel.udt.nio.NioUdtMessageConnectorChannel;

@Immutable
public class NioNettyUdtChannelType implements INettyUdtChannelType {

    public static final NioNettyUdtChannelType INSTANCE = new NioNettyUdtChannelType();

    @Override
    public EventLoopGroup newServerWorkerGroup() {
        return new NioEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new NioEventLoopGroup(1);
    }

    @Override
    public Class<? extends UdtServerChannel> getServerChannelType() {
        return NioUdtMessageAcceptorChannel.class;
    }

    @Override
    public Class<? extends UdtChannel> getClientChannelType() {
        return NioUdtMessageConnectorChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize) {
        consumer.option(ChannelOption.SO_SNDBUF, socketSize);
        consumer.option(ChannelOption.SO_RCVBUF, ByteBuffers
                .calculateExpansion(socketSize * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER));
    }

    @Override
    public void initChannel(final UdtChannel channel, final boolean server) throws Exception {
        //noop
    }
}
