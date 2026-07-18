package de.invesdwin.context.integration.channel.hadronio.netty;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientHandlerFactory;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerHandlerFactory;
import de.invesdwin.context.integration.channel.netty.async.tcp.NettySocketAsynchronousChannel;
import de.invesdwin.context.integration.channel.netty.sync.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.netty.sync.tcp.type.INettySocketChannelType;
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
        final boolean lowLatency = true;
        final NettySocketSynchronousChannel serverChannel = newNettySocketChannel(type, address, true,
                getMaxMessageSize(), lowLatency);
        final NettySocketSynchronousChannel clientChannel = newNettySocketChannel(type, address, false,
                getMaxMessageSize(), lowLatency);
        final NettySocketAsynchronousChannel serverHandler = new NettySocketAsynchronousChannel(serverChannel,
                newSerdeHandlerFactory(new LatencyServerHandlerFactory(this)), false);
        final NettySocketAsynchronousChannel clientHandler = new NettySocketAsynchronousChannel(clientChannel,
                newSerdeHandlerFactory(new LatencyClientHandlerFactory(this)), false);
        new LatencyChannelTest(this).runHandlerLatencyTest(serverHandler, clientHandler);
    }

    protected NettySocketSynchronousChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize,
            final boolean lowLatency) {
        return new NettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

}
