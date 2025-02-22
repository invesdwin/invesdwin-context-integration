package de.invesdwin.context.integration.channel.sync.socket.udp;

import java.io.IOException;
import java.net.ConnectException;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class DatagramSynchronousChannel implements ISessionlessSynchronousChannel<SocketAddress> {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile boolean socketChannelOpening;
    protected final SocketAddress socketAddress;
    protected SocketAddress otherSocketAddress;
    protected final boolean server;
    protected final boolean lowLatency;
    private final SocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();
    private boolean multipleClientsAllowed;

    public DatagramSynchronousChannel(final SocketAddress socketAddress, final boolean server, final int maxMessageSize,
            final boolean lowLatency) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = maxMessageSize;
        this.socketSize = newSocketSize(maxMessageSize);
        this.lowLatency = lowLatency;
        this.finalizer = new SocketSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    protected int newSocketSize(final int maxMessageSize) {
        return maxMessageSize + DatagramSynchronousChannel.MESSAGE_INDEX;
    }

    public void setMultipleClientsAllowed() {
        Assertions.checkTrue(isServer(), "only relevant for server channel");
        this.multipleClientsAllowed = true;
    }

    public boolean isMultipleClientsAllowed() {
        return multipleClientsAllowed;
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    @Override
    public void setOtherSocketAddress(final SocketAddress otherSocketAddress) {
        this.otherSocketAddress = otherSocketAddress;
    }

    @Override
    public SocketAddress getOtherSocketAddress() {
        return otherSocketAddress;
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

    public DatagramSocket getSocket() {
        return finalizer.socket;
    }

    public DatagramChannel getSocketChannel() {
        return finalizer.socketChannel;
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
                finalizer.socketChannel = DatagramChannel.open();
                finalizer.socketChannel.bind(socketAddress);
                finalizer.socket = finalizer.socketChannel.socket();
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                while (true) {
                    try {
                        finalizer.socketChannel = DatagramChannel.open();
                        finalizer.socketChannel.connect(socketAddress);
                        finalizer.socket = finalizer.socketChannel.socket();
                        break;
                    } catch (final ConnectException e) {
                        finalizer.socketChannel.close();
                        finalizer.socketChannel = null;
                        if (finalizer.socket != null) {
                            finalizer.socket.close();
                            finalizer.socket = null;
                        }
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
            //non-blocking datagrams are a lot faster than blocking ones
            finalizer.socketChannel.configureBlocking(false);
            configureSocket(finalizer.socket);
        } finally {
            socketChannelOpening = false;
        }
    }

    protected void configureSocket(final DatagramSocket socket) throws SocketException {
        BlockingDatagramSynchronousChannel.configureSocketStatic(socket, socketSize, lowLatency);
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
        if (isReaderRegistered() && isWriterRegistered()) {
            //bidi readers+writers on same channel have problems with closing when they are not closed on first request
            synchronized (this) {
                if (activeCount.get() > 0) {
                    activeCount.decrementAndGet();
                }
            }
        } else {
            if (!shouldClose()) {
                return;
            }
        }
        finalizer.close();
        otherSocketAddress = null;
    }

    private synchronized boolean shouldClose() {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        return activeCountBefore == 1;
    }

    private static final class SocketSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile DatagramChannel socketChannel;
        private volatile DatagramSocket socket;

        @Override
        protected void clean() {
            if (socketChannel != null) {
                Closeables.closeQuietly(socketChannel);
                socketChannel = null;
            }
            if (socket != null) {
                socket.close();
                socket = null;
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
