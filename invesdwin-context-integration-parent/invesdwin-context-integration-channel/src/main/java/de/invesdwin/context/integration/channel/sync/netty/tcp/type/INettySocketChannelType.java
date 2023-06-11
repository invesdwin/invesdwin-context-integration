package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.context.integration.channel.sync.netty.SelectStrategyFactories;
import de.invesdwin.util.lang.OperatingSystem;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

public interface INettySocketChannelType {

    SelectStrategyFactories DEFAULT_SELECT_STRATEGY = SelectStrategyFactories.BUSY_WAIT;

    static INettySocketChannelType getDefault() {
        if (OperatingSystem.isMac()) {
            return KQueueNettySocketChannelType.INSTANCE;
        } else if (OperatingSystem.isLinux()) {
            return EpollNettySocketChannelType.INSTANCE;
        } else {
            return NioNettySocketChannelType.INSTANCE;
        }
    }

    EventLoopGroup newServerAcceptorGroup(int threadCount);

    EventLoopGroup newServerWorkerGroup(int threadCount, EventLoopGroup bossGroup);

    EventLoopGroup newClientWorkerGroup(int threadCount);

    Class<? extends ServerSocketChannel> getServerChannelType();

    Class<? extends SocketChannel> getClientChannelType();

    void channelOptions(IChannelOptionConsumer consumer, int socketSize, boolean server);

    void initChannel(SocketChannel channel, boolean server) throws Exception;

}
