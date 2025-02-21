package de.invesdwin.context.integration.channel.sync.netty.udp.type;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.util.lang.OperatingSystem;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.socket.DatagramChannel;

public interface INettyDatagramChannelType {

    static INettyDatagramChannelType getDefault() {
        if (OperatingSystem.isMac() && KQueue.isAvailable()) {
            return KQueueNettyDatagramChannelType.INSTANCE;
        } else if (OperatingSystem.isLinux() && Epoll.isAvailable()) {
            return EpollNettyDatagramChannelType.INSTANCE;
        } else {
            return NioNettyDatagramChannelType.INSTANCE;
        }
    }

    EventLoopGroup newServerWorkerGroup(int threadCount, SelectStrategyFactory selectStrategyFactory);

    EventLoopGroup newClientWorkerGroup(int threadCount, SelectStrategyFactory selectStrategyFactory);

    Class<? extends DatagramChannel> getClientChannelType();

    Class<? extends DatagramChannel> getServerChannelType();

    void channelOptions(IChannelOptionConsumer consumer, int socketSize, boolean lowLatency, boolean server);

    void initChannel(DatagramChannel channel, boolean server);

}
