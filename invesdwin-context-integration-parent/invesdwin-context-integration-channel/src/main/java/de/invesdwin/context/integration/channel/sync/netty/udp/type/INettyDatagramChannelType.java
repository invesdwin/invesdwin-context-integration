package de.invesdwin.context.integration.channel.sync.netty.udp.type;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.util.lang.OperatingSystem;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;

public interface INettyDatagramChannelType {

    static INettyDatagramChannelType getDefault() {
        if (OperatingSystem.isMac()) {
            return KQueueNettyDatagramChannelType.INSTANCE;
        } else if (OperatingSystem.isLinux()) {
            return EpollNettyDatagramChannelType.INSTANCE;
        } else {
            return NioNettyDatagramChannelType.INSTANCE;
        }
    }

    EventLoopGroup newServerWorkerGroup();

    EventLoopGroup newClientWorkerGroup();

    Class<? extends DatagramChannel> getClientChannelType();

    Class<? extends DatagramChannel> getServerChannelType();

    void channelOptions(IChannelOptionConsumer consumer, int socketSize);

    void initChannel(DatagramChannel channel, boolean server) throws Exception;

}
