package de.invesdwin.context.integration.channel.sync.hadronio.netty;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientHandlerFactory;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerHandlerFactory;
import de.invesdwin.context.integration.channel.async.netty.tcp.NettySocketAsynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;

@Immutable
public class HadronioNettySocketHandlerTest extends AChannelTest {

    @Test
    public void testNettySocketHandlerPerformance() throws InterruptedException {
        final String addr = findLocalNetworkAddress();
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress(addr, port);
        runNettySocketHandlerPerformanceTest(HadroNioNettySocketChannelType.INSTANCE, address);
    }

    private void runNettySocketHandlerPerformanceTest(final INettySocketChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final NettySocketSynchronousChannel serverChannel = newNettySocketChannel(type, address, true,
                getMaxMessageSize());
        final NettySocketSynchronousChannel clientChannel = newNettySocketChannel(type, address, false,
                getMaxMessageSize());
        final NettySocketAsynchronousChannel serverHandler = new NettySocketAsynchronousChannel(serverChannel,
                newSerdeHandlerFactory(new LatencyServerHandlerFactory()), false);
        final NettySocketAsynchronousChannel clientHandler = new NettySocketAsynchronousChannel(clientChannel,
                newSerdeHandlerFactory(new LatencyClientHandlerFactory()), false);
        new LatencyChannelTest(this).runHandlerLatencyTest(serverHandler, clientHandler);
    }

    protected NettySocketSynchronousChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
