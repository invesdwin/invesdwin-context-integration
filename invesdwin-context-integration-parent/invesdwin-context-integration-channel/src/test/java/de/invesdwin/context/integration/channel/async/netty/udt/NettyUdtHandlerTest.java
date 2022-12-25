package de.invesdwin.context.integration.channel.async.netty.udt;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.netty.udt.NettyUdtSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.INettyUdtChannelType;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.NioNettyUdtChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;

@Immutable
public class NettyUdtHandlerTest extends AChannelTest {

    @Test
    public void testNettyUdtHandlerPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableUdpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runNettyUdtHandlerPerformanceTest(NioNettyUdtChannelType.INSTANCE, address);
    }

    private void runNettyUdtHandlerPerformanceTest(final INettyUdtChannelType type, final InetSocketAddress address)
            throws InterruptedException {
        final NettyUdtSynchronousChannel serverChannel = new NettyUdtSynchronousChannel(type, address, true,
                getMaxMessageSize());
        final NettyUdtSynchronousChannel clientChannel = new NettyUdtSynchronousChannel(type, address, false,
                getMaxMessageSize());
        final NettyUdtAsynchronousChannel serverHandler = new NettyUdtAsynchronousChannel(serverChannel,
                newCommandHandler(new WriterHandler()));
        final NettyUdtAsynchronousChannel clientHandler = new NettyUdtAsynchronousChannel(clientChannel,
                newCommandHandler(new ReaderHandler()));
        runHandlerPerformanceTest(serverHandler, clientHandler);
    }

}
