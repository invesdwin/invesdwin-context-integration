package de.invesdwin.context.integration.channel.async.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.EpollNettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;

@Immutable
public class NettySocketHandlerTest extends AChannelTest {

    @Test
    public void testNettySocketHandlerPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runNettySocketHandlerPerformanceTest(EpollNettySocketChannelType.INSTANCE, address);
    }

    private void runNettySocketHandlerPerformanceTest(final INettySocketChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final NettySocketChannel serverChannel = newNettySocketChannel(type, address, true, getMaxMessageSize());
        final NettySocketChannel clientChannel = newNettySocketChannel(type, address, false, getMaxMessageSize());
        final NettySocketAsynchronousChannel serverHandler = new NettySocketAsynchronousChannel(serverChannel,
                newCommandHandler(new WriterHandler()));
        final NettySocketAsynchronousChannel clientHandler = new NettySocketAsynchronousChannel(clientChannel,
                newCommandHandler(new ReaderHandler()));
        runHandlerPerformanceTest(serverHandler, clientHandler);
    }

    protected NettySocketChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettySocketChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
