package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.context.integration.channel.sync.netty.SelectStrategyFactories;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

public interface INettySocketChannelType {

    SelectStrategyFactories DEFAULT_SELECT_STRATEGY = SelectStrategyFactories.BUSY_WAIT;

    EventLoopGroup newServerAcceptorGroup();

    EventLoopGroup newServerWorkerGroup(EventLoopGroup bossGroup);

    EventLoopGroup newClientWorkerGroup();

    Class<? extends ServerSocketChannel> getServerChannelType();

    Class<? extends SocketChannel> getClientChannelType();

    void channelOptions(IChannelOptionConsumer consumer, int socketSize);

    void initChannel(SocketChannel channel, boolean server) throws Exception;

}
