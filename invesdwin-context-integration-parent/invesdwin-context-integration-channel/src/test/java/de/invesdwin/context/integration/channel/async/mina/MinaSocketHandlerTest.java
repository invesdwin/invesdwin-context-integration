package de.invesdwin.context.integration.channel.async.mina;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientHandlerFactory;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerHandlerFactory;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.context.integration.channel.sync.mina.type.MinaSocketType;
import de.invesdwin.context.integration.network.NetworkUtil;

@Immutable
public class MinaSocketHandlerTest extends AChannelTest {

    @Test
    public void testMinaSocketHandlerPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runMinaSocketHandlerPerformanceTest(MinaSocketType.NioTcp, address);
    }

    private void runMinaSocketHandlerPerformanceTest(final IMinaSocketType type, final InetSocketAddress address)
            throws InterruptedException {
        final MinaSocketSynchronousChannel serverChannel = newMinaSocketChannel(type, address, true,
                getMaxMessageSize());
        final MinaSocketSynchronousChannel clientChannel = newMinaSocketChannel(type, address, false,
                getMaxMessageSize());
        final MinaSocketAsynchronousChannel serverHandler = new MinaSocketAsynchronousChannel(serverChannel,
                newSerdeHandlerFactory(new LatencyServerHandlerFactory(this)), false);
        final MinaSocketAsynchronousChannel clientHandler = new MinaSocketAsynchronousChannel(clientChannel,
                newSerdeHandlerFactory(new LatencyClientHandlerFactory(this)), false);
        new LatencyChannelTest(this).runHandlerLatencyTest(serverHandler, clientHandler);
    }

    protected MinaSocketSynchronousChannel newMinaSocketChannel(final IMinaSocketType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new MinaSocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
