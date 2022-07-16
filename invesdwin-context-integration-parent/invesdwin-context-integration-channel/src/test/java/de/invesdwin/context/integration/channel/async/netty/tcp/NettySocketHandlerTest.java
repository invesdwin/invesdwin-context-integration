package de.invesdwin.context.integration.channel.async.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.EpollNettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;

@Immutable
public class NettySocketHandlerTest extends AChannelTest {

    @Test
    public void testNettySocketHandlerPerformance() throws InterruptedException {
        final InetSocketAddress address = new InetSocketAddress("localhost", 7878);
        runNettySocketHandlerPerformanceTest(EpollNettySocketChannelType.INSTANCE, address);
    }

    private void runNettySocketHandlerPerformanceTest(final INettySocketChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final NettySocketChannel serverChannel = new NettySocketChannel(type, address, true, getMaxMessageSize());
        final NettySocketChannel clientChannel = new NettySocketChannel(type, address, false, getMaxMessageSize());
        final NettySocketAsynchronousChannel serverHandler = new NettySocketAsynchronousChannel(serverChannel,
                newCommandHandler(new WriterHandler()));
        final NettySocketAsynchronousChannel clientHandler = new NettySocketAsynchronousChannel(clientChannel,
                newCommandHandler(new ReaderHandler()));
        runHandlerPerformanceTest(serverHandler, clientHandler);
    }

}
