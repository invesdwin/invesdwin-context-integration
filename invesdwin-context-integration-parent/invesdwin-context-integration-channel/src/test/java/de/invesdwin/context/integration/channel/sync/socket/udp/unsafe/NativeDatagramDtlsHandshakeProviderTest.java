package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.IHandshakeProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.TlsHandshakeProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.ITlsProtocol;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.TlsProtocol;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class NativeDatagramDtlsHandshakeProviderTest extends AChannelTest {

    @Test
    public void testBidiNioSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNativeDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    protected void runNativeDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final InetSocketAddress address = new InetSocketAddress("localhost", 8080);
        final HandshakeChannelFactory serverHandshake = new HandshakeChannelFactory(
                newTlsHandshakeProvider(MAX_WAIT_DURATION, address, true));
        final HandshakeChannelFactory clientHandshake = new HandshakeChannelFactory(
                newTlsHandshakeProvider(MAX_WAIT_DURATION, address, false));

        final boolean lowLatency = true;
        final ISynchronousWriter<IByteBufferProvider> responseWriter = serverHandshake
                .newWriter(new NativeDatagramSynchronousWriter(
                        newDatagramSynchronousChannel(responseAddress, false, getMaxMessageSize(), lowLatency)));
        final ISynchronousReader<IByteBufferProvider> requestReader = serverHandshake
                .newReader(new NativeDatagramSynchronousReader(
                        newDatagramSynchronousChannel(requestAddress, true, getMaxMessageSize(), lowLatency)));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = clientHandshake
                .newWriter(new NativeDatagramSynchronousWriter(
                        newDatagramSynchronousChannel(requestAddress, false, getMaxMessageSize(), lowLatency)));
        final ISynchronousReader<IByteBufferProvider> responseReader = clientHandshake
                .newReader(new NativeDatagramSynchronousReader(
                        newDatagramSynchronousChannel(responseAddress, true, getMaxMessageSize(), lowLatency)));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected DatagramSynchronousChannel newDatagramSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        return new DatagramSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

    private IHandshakeProvider newTlsHandshakeProvider(final Duration handshakeTimeout,
            final InetSocketAddress socketAddress, final boolean server) {
        return new TlsHandshakeProvider(handshakeTimeout, socketAddress, server) {
            @Override
            protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider() {
                return new DerivedKeyTransportLayerSecurityProvider(getSocketAddress(), isServer()) {
                    @Override
                    protected String getHostname() {
                        return getSocketAddress().getHostName();
                    }

                    @Override
                    protected Integer getMaximumPacketSize() {
                        return NativeDatagramDtlsHandshakeProviderTest.this.getMaxMessageSize()
                                - DatagramSynchronousChannel.MESSAGE_INDEX;
                    }

                    @Override
                    public ITlsProtocol getProtocol() {
                        return TlsProtocol.DTLS;
                    }
                };
            }
        };
    }

    @Override
    public int getMaxMessageSize() {
        return SynchronousChannels.MAX_UNFRAGMENTED_DATAGRAM_PACKET_SIZE;
    }

}
