package de.invesdwin.context.integration.channel.sync.netty.udt.type;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.udt.UdtChannel;
import io.netty.channel.udt.UdtServerChannel;

public interface INettyUdtChannelType {

    EventLoopGroup newServerWorkerGroup();

    EventLoopGroup newClientWorkerGroup();

    Class<? extends UdtChannel> getClientChannelType();

    Class<? extends UdtServerChannel> getServerChannelType();

    void channelOptions(IChannelOptionConsumer consumer, int socketSize);

    void initChannel(UdtChannel channel, boolean server) throws Exception;

}
