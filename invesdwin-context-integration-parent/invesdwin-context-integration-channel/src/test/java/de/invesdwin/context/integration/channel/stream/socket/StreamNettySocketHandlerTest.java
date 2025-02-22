package de.invesdwin.context.integration.channel.stream.socket;

import java.net.InetSocketAddress;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.netty.tcp.NettySocketAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.stream.StreamLatencyChannelTest;
import de.invesdwin.context.integration.channel.stream.server.async.StreamAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySharedSocketEndpointFactory;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class StreamNettySocketHandlerTest extends AChannelTest {

    @Test
    public void testStreamPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        final INettySocketChannelType type = INettySocketChannelType.getDefault();
        final boolean lowLatency = true;
        final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory = new Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel>() {
            @Override
            public IAsynchronousChannel apply(final StreamAsynchronousEndpointServerHandlerFactory t) {
                final NettySocketSynchronousChannel channel = new NettySocketSynchronousChannel(type, address, true,
                        getMaxMessageSize(), lowLatency) {
                    @Override
                    protected int newServerWorkerGroupThreadCount() {
                        return StreamLatencyChannelTest.STREAM_CLIENT_TRANSPORTS;
                    }
                };
                return new NettySocketAsynchronousChannel(channel, t, true);
            }
        };
        //netty shared bootstrap
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NettySharedSocketEndpointFactory(
                type, address, getMaxMessageSize(), lowLatency) {
            @Override
            protected int newClientWorkerGroupThreadCount() {
                return StreamLatencyChannelTest.STREAM_CLIENT_TRANSPORTS;
            }
        };
        //netty no shared bootstrap
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NettySocketClientEndpointFactory(
        //                type, address, getMaxMessageSize());
        //fastest
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NativeSocketClientEndpointFactory(
        //                address, getMaxMessageSize());
        new StreamLatencyChannelTest(this).runStreamHandlerLatencyTest(serverFactory, clientEndpointFactory);
    }

    @Override
    public int getMaxMessageSize() {
        return super.getMaxMessageSize() + ServiceSynchronousCommandSerde.MESSAGE_INDEX;
    }

}
