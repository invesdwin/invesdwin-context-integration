package de.invesdwin.context.integration.channel.netty.type;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.SocketChannel;

public interface INettyChannelType {

    EventLoopGroup newServerBossGroup();

    EventLoopGroup newServerWorkerGroup(EventLoopGroup bossGroup);

    Class<? extends ServerChannel> getServerChannelType();

    Class<? extends Channel> getClientChannelType();

    void channelOptions(IChannelOptionConsumer consumer, int socketSize);

    EventLoopGroup newClientWorkerGroup();

    void initChannel(SocketChannel channel, boolean server) throws Exception;

}
