package de.invesdwin.context.integration.channel.sync.netty.udt.type;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.udt.UdtChannel;
import io.netty.channel.udt.UdtServerChannel;

public interface INettyUdtChannelType {

    EventLoopGroup newServerAcceptorGroup(int threadCount);

    EventLoopGroup newServerWorkerGroup(int threadCount, EventLoopGroup parentGroup);

    EventLoopGroup newClientWorkerGroup(int threadCount);

    ChannelFactory<? extends UdtChannel> getClientChannelFactory();

    ChannelFactory<? extends UdtServerChannel> getServerChannelFactory();

    void channelOptions(IChannelOptionConsumer consumer, int socketSize, boolean server);

    void initChannel(UdtChannel channel, boolean server);

}
