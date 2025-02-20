package de.invesdwin.context.integration.channel.stream.socket.sessionless;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.stream.AStreamLatencyChannelTest;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceFactory;
import de.invesdwin.context.integration.channel.sync.socket.udp.unsafe.NativeDatagramEndpointFactory;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class StreamSessionlessNativeDatagramChannelTest extends AStreamLatencyChannelTest {

    @Test
    public void testStreamPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableUdpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory = new NativeDatagramEndpointFactory(
                address, true, getMaxMessageSize());
        final IStreamSynchronousEndpointServiceFactory serviceFactory = newServiceFactory();
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NativeDatagramEndpointFactory(
                address, false, getMaxMessageSize());
        runStreamSessionlessPerformanceTest(serverEndpointFactory, serviceFactory, clientEndpointFactory);
    }

    @Override
    protected int getMaxMessageSize() {
        return super.getMaxMessageSize() + ServiceSynchronousCommandSerde.MESSAGE_INDEX;
    }

}
