package de.invesdwin.context.integration.channel.async.netty.udp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientHandlerFactory;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerHandlerFactory;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.NioNettyDatagramChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;

@Immutable
public class NettyDatagramHandlerTest extends AChannelTest {

    @Test
    public void testNettyDatagramHandlerPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableUdpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runNettyDatagramHandlerPerformanceTest(NioNettyDatagramChannelType.INSTANCE, address);
    }

    private void runNettyDatagramHandlerPerformanceTest(final INettyDatagramChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final boolean lowLatency = true;
        final NettyDatagramSynchronousChannel serverChannel = new NettyDatagramSynchronousChannel(type, address, true,
                getMaxMessageSize(), lowLatency);
        final NettyDatagramSynchronousChannel clientChannel = new NettyDatagramSynchronousChannel(type, address, false,
                getMaxMessageSize(), lowLatency);
        final NettyDatagramAsynchronousChannel serverHandler = new NettyDatagramAsynchronousChannel(serverChannel,
                newSerdeHandlerFactory(new LatencyServerHandlerFactory()), false);
        final NettyDatagramAsynchronousChannel clientHandler = new NettyDatagramAsynchronousChannel(clientChannel,
                newSerdeHandlerFactory(new LatencyClientHandlerFactory()), false);
        new LatencyChannelTest(this).runHandlerLatencyTest(serverHandler, clientHandler);
    }

}
