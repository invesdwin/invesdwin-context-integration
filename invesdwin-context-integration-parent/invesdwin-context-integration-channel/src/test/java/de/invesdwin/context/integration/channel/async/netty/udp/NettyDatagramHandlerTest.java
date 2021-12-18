package de.invesdwin.context.integration.channel.async.netty.udp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.EpollNettyDatagramChannelType;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;

@Immutable
public class NettyDatagramHandlerTest extends AChannelTest {

    @Test
    public void testNettyDatagramHandlerPerformance() throws InterruptedException {
        final InetSocketAddress address = new InetSocketAddress("localhost", 7878);
        runNettyDatagramHandlerPerformanceTest(EpollNettyDatagramChannelType.INSTANCE, address);
    }

    private void runNettyDatagramHandlerPerformanceTest(final INettyDatagramChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final NettyDatagramChannel serverChannel = new NettyDatagramChannel(type, address, true, MESSAGE_SIZE);
        final NettyDatagramChannel clientChannel = new NettyDatagramChannel(type, address, false, MESSAGE_SIZE);
        final NettyDatagramAsynchronousChannel serverHandler = new NettyDatagramAsynchronousChannel(serverChannel,
                newCommandHandler(new WriterHandler()));
        final NettyDatagramAsynchronousChannel clientHandler = new NettyDatagramAsynchronousChannel(clientChannel,
                newCommandHandler(new ReaderHandler()));
        runHandlerPerformanceTest(serverHandler, clientHandler);
    }

}
