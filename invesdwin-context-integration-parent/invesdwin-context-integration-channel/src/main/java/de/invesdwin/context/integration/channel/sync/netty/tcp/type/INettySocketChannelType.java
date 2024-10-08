package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.util.lang.OperatingSystem;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

public interface INettySocketChannelType {

    static INettySocketChannelType getDefault() {
        if (OperatingSystem.isMac() && KQueue.isAvailable()) {
            return KQueueNettySocketChannelType.INSTANCE;
        } else if (OperatingSystem.isLinux() && Epoll.isAvailable()) {
            return EpollNettySocketChannelType.INSTANCE;
        } else {
            return NioNettySocketChannelType.INSTANCE;
        }
    }

    EventLoopGroup newServerAcceptorGroup(int threadCount);

    EventLoopGroup newServerWorkerGroup(int threadCount, SelectStrategyFactory selectStrategyFactory,
            EventLoopGroup bossGroup);

    EventLoopGroup newClientWorkerGroup(int threadCount, SelectStrategyFactory selectStrategyFactory);

    Class<? extends ServerSocketChannel> getServerChannelType();

    Class<? extends SocketChannel> getClientChannelType();

    void channelOptions(IChannelOptionConsumer consumer, int socketSize, boolean server);

    void initChannel(SocketChannel channel, boolean server);

}
