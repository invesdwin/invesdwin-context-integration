package de.invesdwin.context.integration.channel.stream.socket;

import java.net.InetSocketAddress;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.netty.udp.NettyDatagramAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.stream.AStreamLatencyChannelTest;
import de.invesdwin.context.integration.channel.stream.server.async.StreamAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceFactory;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettySharedDatagramClientEndpointFactory;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class StreamNettyDatagramHandlerTest extends AStreamLatencyChannelTest {

    @Test
    public void testRpcPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        final INettyDatagramChannelType type = INettyDatagramChannelType.getDefault();
        final Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory = new Function<StreamAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel>() {
            @Override
            public IAsynchronousChannel apply(final StreamAsynchronousEndpointServerHandlerFactory t) {
                final NettyDatagramSynchronousChannel channel = new NettyDatagramSynchronousChannel(type, address, true,
                        getMaxMessageSize()) {
                    @Override
                    protected int newServerWorkerGroupThreadCount() {
                        return STREAM_CLIENT_TRANSPORTS;
                    }
                };
                return new NettyDatagramAsynchronousChannel(channel, t, true);
            }
        };
        final IStreamSynchronousEndpointServiceFactory serviceFactory = newServiceFactory();
        //netty shared bootstrap
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NettySharedDatagramClientEndpointFactory(
                type, address, getMaxMessageSize()) {
            @Override
            protected int newClientWorkerGroupThreadCount() {
                return STREAM_CLIENT_TRANSPORTS;
            }
        };
        //netty no shared bootstrap
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NettyDatagramClientEndpointFactory(
        //                type, address, getMaxMessageSize());
        //fastest
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NativeDatagramClientEndpointFactory(
        //                address, getMaxMessageSize());
        runStreamHandlerPerformanceTest(serverFactory, serviceFactory, clientEndpointFactory);
    }

    @Override
    protected int getMaxMessageSize() {
        return super.getMaxMessageSize() + ServiceSynchronousCommandSerde.MESSAGE_INDEX;
    }

}
