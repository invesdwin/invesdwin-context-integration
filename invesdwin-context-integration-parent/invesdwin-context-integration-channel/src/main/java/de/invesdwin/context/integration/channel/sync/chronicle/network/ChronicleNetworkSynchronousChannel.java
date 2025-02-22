package de.invesdwin.context.integration.channel.sync.chronicle.network;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.chronicle.network.type.ChronicleSocketChannelType;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.time.duration.Duration;
import net.openhft.chronicle.network.tcp.ChronicleServerSocket;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocket;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

@NotThreadSafe
public class ChronicleNetworkSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final ChronicleSocketChannelType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile boolean socketChannelOpening;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    protected final boolean lowLatency;
    private final SocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();

    public ChronicleNetworkSynchronousChannel(final ChronicleSocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize,
            final boolean lowLatency) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = newSocketSize(estimatedMaxMessageSize);
        this.lowLatency = lowLatency;
        this.finalizer = new SocketSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    protected int newSocketSize(final int estimatedMaxMessageSize) {
        return estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public boolean isServer() {
        return server;
    }

    public boolean isLowLatency() {
        return lowLatency;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public ChronicleSocket getSocket() {
        return finalizer.socket;
    }

    public ChronicleSocketChannel getSocketChannel() {
        return finalizer.socketChannel;
    }

    public ChronicleServerSocketChannel getServerSocketChannel() {
        return finalizer.serverSocketChannel;
    }

    public boolean isReaderRegistered() {
        return readerRegistered;
    }

    public void setReaderRegistered() {
        if (readerRegistered) {
            throw new IllegalStateException("reader already registered");
        }
        this.readerRegistered = true;
    }

    public boolean isWriterRegistered() {
        return writerRegistered;
    }

    public void setWriterRegistered() {
        if (writerRegistered) {
            throw new IllegalStateException("writer already registered");
        }
        this.writerRegistered = true;
    }

    @Override
    public void open() throws IOException {
        if (!shouldOpen()) {
            awaitSocketChannel();
            return;
        }
        socketChannelOpening = true;
        try {
            if (server) {
                finalizer.serverSocketChannel = type.newServerSocketChannel(socketAddress.toString());
                finalizer.serverSocketChannel.bind(socketAddress);
                finalizer.serverSocket = finalizer.serverSocketChannel.socket();
                finalizer.socketChannel = type.acceptSocketChannel(finalizer.serverSocketChannel);
                finalizer.socket = finalizer.socketChannel.socket();
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                while (true) {
                    finalizer.socketChannel = type.newSocketChannel(SocketChannel.open());
                    try {
                        finalizer.socketChannel.connect(socketAddress);
                        finalizer.socket = finalizer.socketChannel.socket();
                        break;
                    } catch (final ConnectException e) {
                        finalizer.socketChannel.close();
                        finalizer.socketChannel = null;
                        finalizer.socket = null;
                        if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                            try {
                                getMaxConnectRetryDelay().sleepRandom();
                            } catch (final InterruptedException e1) {
                                throw new IOException(e1);
                            }
                        } else {
                            throw e;
                        }
                    }
                }
            }
            //non-blocking sockets are a bit faster than blocking ones
            finalizer.socketChannel.configureBlocking(lowLatency);
            configureSocketStatic(finalizer.socket, socketSize, readerRegistered);
        } finally {
            socketChannelOpening = false;
        }
    }

    public static void configureSocketStatic(final ChronicleSocket socket, final int socketSize,
            final boolean lowLatency) throws SocketException {
        if (lowLatency) {
            socket.setReceiveBufferSize(Integers.max(socket.getReceiveBufferSize(), ByteBuffers.calculateExpansion(
                    socketSize * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER)));
            socket.setSendBufferSize(socketSize);
            socket.setTcpNoDelay(true);
        } else {
            socket.setReceiveBufferSize(Integers.max(socket.getReceiveBufferSize(), ByteBuffers.calculateExpansion(
                    socketSize * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER)));
            socket.setSendBufferSize(Integers.max(socket.getSendBufferSize(), ByteBuffers.calculateExpansion(
                    socketSize * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER)));
            socket.setTcpNoDelay(false);
        }
    }

    private void awaitSocketChannel() throws IOException {
        try {
            //wait for channel
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while ((finalizer.socketChannel == null || socketChannelOpening) && activeCount.get() > 0) {
                if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                    getWaitInterval().sleep();
                } else {
                    throw new ConnectException("Connection timeout");
                }
            }
        } catch (final Throwable t) {
            close();
            throw new IOException(t);
        }
    }

    private synchronized boolean shouldOpen() {
        return activeCount.incrementAndGet() == 1;
    }

    protected SocketChannel newSocketChannel() throws IOException {
        return SocketChannel.open();
    }

    protected ServerSocketChannel newServerSocketChannel() throws IOException {
        return ServerSocketChannel.open();
    }

    protected Duration getMaxConnectRetryDelay() {
        return SynchronousChannels.DEFAULT_MAX_RECONNECT_DELAY;
    }

    protected Duration getConnectTimeout() {
        return SynchronousChannels.DEFAULT_CONNECT_TIMEOUT;
    }

    protected Duration getWaitInterval() {
        return SynchronousChannels.DEFAULT_WAIT_INTERVAL;
    }

    @Override
    public void close() {
        if (!shouldClose()) {
            return;
        }
        finalizer.close();
    }

    private synchronized boolean shouldClose() {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        return activeCountBefore == 1;
    }

    private static final class SocketSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile ChronicleSocket socket;
        private volatile ChronicleSocketChannel socketChannel;
        private volatile ChronicleSocketChannelType type;
        private volatile ChronicleServerSocketChannel serverSocketChannel;
        private volatile ChronicleServerSocket serverSocket;

        @Override
        protected void clean() {
            if (socketChannel != null) {
                socketChannel.close();
                socketChannel = null;
            }
            socket = null;
            if (serverSocketChannel != null) {
                serverSocketChannel.close();
                serverSocketChannel = null;
            }
            if (serverSocket != null) {
                serverSocket.close();
                serverSocket = null;
            }
        }

        @Override
        protected boolean isCleaned() {
            return socketChannel == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

}
