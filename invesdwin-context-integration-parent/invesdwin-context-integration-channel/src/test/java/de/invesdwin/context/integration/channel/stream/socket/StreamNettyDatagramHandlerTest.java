package de.invesdwin.context.integration.channel.stream.socket;

import java.net.InetSocketAddress;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.netty.udp.NettyDatagramAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.stream.StreamLatencyChannelTest;
import de.invesdwin.context.integration.channel.stream.server.async.StreamAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.context.integration.channel.sync.socket.udp.unsafe.NativeDatagramEndpointFactory;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class StreamNettyDatagramHandlerTest extends AChannelTest {

    @Test
    public void testStreamPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        final INettyDatagramChannelType type = INettyDatagramChannelType.getDefault();
        final boolean lowLatency = true;
        final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory = new Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel>() {
            @Override
            public IAsynchronousChannel apply(final StreamAsynchronousEndpointServerHandlerFactory t) {
                final NettyDatagramSynchronousChannel channel = new NettyDatagramSynchronousChannel(type, address, true,
                        getMaxMessageSize(), lowLatency) {
                    @Override
                    protected int newServerWorkerGroupThreadCount() {
                        return StreamLatencyChannelTest.STREAM_CLIENT_TRANSPORTS;
                    }
                };
                return new NettyDatagramAsynchronousChannel(channel, t, true);
            }
        };
        //netty shared bootstrap
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NettySharedDatagramEndpointFactory(
        //                type, address, getMaxMessageSize(), lowLatency) {
        //            @Override
        //            protected int newClientWorkerGroupThreadCount() {
        //                return StreamLatencyChannelTest.STREAM_CLIENT_TRANSPORTS;
        //            }
        //        };
        //netty no shared bootstrap
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NettyDatagramEndpointFactory(
        //                type, address, false, getMaxMessageSize(), lowLatency);
        //fastest
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NativeDatagramEndpointFactory(
                address, false, getMaxMessageSize(), lowLatency);
        new StreamLatencyChannelTest(this).runStreamHandlerLatencyTest(serverFactory, clientEndpointFactory);
    }

    @Override
    public int getMaxMessageSize() {
        return 42;
    }

}
