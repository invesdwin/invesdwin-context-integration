package de.invesdwin.context.integration.channel.sync.netty.udp.type;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;

public interface INettyDatagramChannelType {

    EventLoopGroup newServerWorkerGroup();

    EventLoopGroup newClientWorkerGroup();

    Class<? extends DatagramChannel> getClientChannelType();

    Class<? extends DatagramChannel> getServerChannelType();

    void channelOptions(IChannelOptionConsumer consumer, int socketSize);

    void initChannel(DatagramChannel channel, boolean server) throws Exception;

}
