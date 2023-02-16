package de.invesdwin.context.integration.channel.sync.socket.udt;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.IHandshakeProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.TlsHandshakeProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.ITlsProtocol;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.TlsProtocol;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class BidiUdtTlsHandshakeProviderTest extends AChannelTest {

    @Test
    public void testBidiNioUdtPerformance() throws InterruptedException {
        final InetSocketAddress address = new InetSocketAddress("localhost", NetworkUtil.findAvailableTcpPort());
        runNioUdtPerformanceTest(address);
    }

    protected void runNioUdtPerformanceTest(final InetSocketAddress address) throws InterruptedException {
        final HandshakeChannelFactory serverHandshake = new HandshakeChannelFactory(
                newTlsHandshakeProvider(MAX_WAIT_DURATION, address, true));
        final HandshakeChannelFactory clientHandshake = new HandshakeChannelFactory(
                newTlsHandshakeProvider(MAX_WAIT_DURATION, address, false));

        final UdtSynchronousChannel serverChannel = newUdtSynchronousChannel(address, true, getMaxMessageSize());
        final UdtSynchronousChannel clientChannel = newUdtSynchronousChannel(address, false, getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = serverHandshake
                .newWriter(new UdtSynchronousWriter(serverChannel));
        final ISynchronousReader<IByteBufferProvider> requestReader = serverHandshake
                .newReader(new UdtSynchronousReader(serverChannel));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBidiUdtPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = clientHandshake
                .newWriter(new UdtSynchronousWriter(clientChannel));
        final ISynchronousReader<IByteBufferProvider> responseReader = clientHandshake
                .newReader(new UdtSynchronousReader(clientChannel));
        new ReaderTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected UdtSynchronousChannel newUdtSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new UdtSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
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
                    public ITlsProtocol getProtocol() {
                        return TlsProtocol.TLS;
                    }
                };
            }
        };
    }

    @Override
    protected int getMaxMessageSize() {
        return 1324;
    }

}
