package de.invesdwin.context.integration.channel.async.netty.tcp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.Immutable;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.NioNettySocketChannelType;

@Immutable
public class NettySocketHandlerTest extends AChannelTest {

    @Test
    public void testNettySocketChannelPerformance() throws InterruptedException {
        final SocketAddress address = new InetSocketAddress("localhost", 7878);
        runNettySocketChannelPerformanceTest(NioNettySocketChannelType.INSTANCE, address);
    }

    private void runNettySocketChannelPerformanceTest(final INettySocketChannelType type, final SocketAddress address)
            throws InterruptedException {
        final NettySocketChannel serverChannel = new NettySocketChannel(type, address, true, MESSAGE_SIZE);
        final NettySocketChannel clientChannel = new NettySocketChannel(type, address, false, MESSAGE_SIZE);
        final NettySocketAsynchronousChannel serverHandler = new NettySocketAsynchronousChannel(serverChannel,
                newCommandHandler(new WriterHandler()));
        final NettySocketAsynchronousChannel clientHandler = new NettySocketAsynchronousChannel(clientChannel,
                newCommandHandler(new ReaderHandler()));
        runHandlerPerformanceTest(serverHandler, clientHandler);
    }

}
