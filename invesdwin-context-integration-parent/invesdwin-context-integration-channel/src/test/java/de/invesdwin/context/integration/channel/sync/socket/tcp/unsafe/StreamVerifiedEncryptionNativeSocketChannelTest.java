package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

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
import de.invesdwin.context.integration.channel.sync.crypto.encryption.EncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.stream.StreamVerifiedEncryptionSynchronousReader;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.stream.StreamVerifiedEncryptionSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.verification.VerificationChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class StreamVerifiedEncryptionNativeSocketChannelTest extends AChannelTest {

    public static final IEncryptionFactory ENCRYPTION_FACTORY = EncryptionChannelTest.ENCRYPTION_FACTORY;
    public static final IVerificationFactory VERIFICATION_FACTORY = VerificationChannelTest.VERIFICATION_FACTORY;

    @Test
    public void testBidiNioSocketPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runNioSocketPerformanceTest(address);
    }

    protected void runNioSocketPerformanceTest(final SocketAddress address) throws InterruptedException {
        final boolean lowLatency = true;
        final SocketSynchronousChannel serverChannel = newSocketSynchronousChannel(address, true, getMaxMessageSize(),
                lowLatency);
        final SocketSynchronousChannel clientChannel = newSocketSynchronousChannel(address, false, getMaxMessageSize(),
                lowLatency);

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new StreamVerifiedEncryptionSynchronousWriter(
                new NativeSocketSynchronousWriter(serverChannel), ENCRYPTION_FACTORY, VERIFICATION_FACTORY);
        final ISynchronousReader<IByteBufferProvider> requestReader = new StreamVerifiedEncryptionSynchronousReader(
                new NativeSocketSynchronousReader(serverChannel), ENCRYPTION_FACTORY, VERIFICATION_FACTORY);
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new StreamVerifiedEncryptionSynchronousWriter(
                new NativeSocketSynchronousWriter(clientChannel), ENCRYPTION_FACTORY, VERIFICATION_FACTORY);
        final ISynchronousReader<IByteBufferProvider> responseReader = new StreamVerifiedEncryptionSynchronousReader(
                new NativeSocketSynchronousReader(clientChannel), ENCRYPTION_FACTORY, VERIFICATION_FACTORY);
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

    @Override
    public int getMaxMessageSize() {
        return 28;
    }

}
