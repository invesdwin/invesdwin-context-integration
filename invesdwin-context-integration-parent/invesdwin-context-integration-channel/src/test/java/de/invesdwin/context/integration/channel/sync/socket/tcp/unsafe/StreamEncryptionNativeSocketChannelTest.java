package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.EncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.stream.StreamEncryptionSynchronousReader;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.stream.StreamEncryptionSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class StreamEncryptionNativeSocketChannelTest extends AChannelTest {

    public static final IEncryptionFactory ENCRYPTION_FACTORY = EncryptionChannelTest.ENCRYPTION_FACTORY;

    @Test
    public void testBidiNioSocketPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runNioSocketPerformanceTest(address);
    }

    protected void runNioSocketPerformanceTest(final SocketAddress address) throws InterruptedException {
        final SocketSynchronousChannel serverChannel = newSocketSynchronousChannel(address, true, getMaxMessageSize());
        final SocketSynchronousChannel clientChannel = newSocketSynchronousChannel(address, false, getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new StreamEncryptionSynchronousWriter(
                new NativeSocketSynchronousWriter(serverChannel), ENCRYPTION_FACTORY);
        final ISynchronousReader<IByteBufferProvider> requestReader = new StreamEncryptionSynchronousReader(
                new NativeSocketSynchronousReader(serverChannel), ENCRYPTION_FACTORY);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBidiSocketPerformance", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new StreamEncryptionSynchronousWriter(
                new NativeSocketSynchronousWriter(clientChannel), ENCRYPTION_FACTORY);
        final ISynchronousReader<IByteBufferProvider> responseReader = new StreamEncryptionSynchronousReader(
                new NativeSocketSynchronousReader(clientChannel), ENCRYPTION_FACTORY);
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    protected int getMaxMessageSize() {
        return 28;
    }

}
