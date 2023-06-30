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

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class DatagramSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile boolean socketChannelOpening;
    protected final SocketAddress socketAddress;
    protected SocketAddress otherSocketAddress;
    protected final boolean server;
    private final SocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();

    public DatagramSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int maxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = maxMessageSize;
        this.socketSize = maxMessageSize;
        this.finalizer = new SocketSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public void setOtherSocketAddress(final SocketAddress otherSocketAddress) {
        if (this.otherSocketAddress != null) {
            throw new IllegalStateException("otherSocketAddress should be null");
        }
        this.otherSocketAddress = otherSocketAddress;
    }

    public SocketAddress getOtherSocketAddress() {
        return otherSocketAddress;
    }

    public boolean isServer() {
        return server;
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
        BlockingDatagramSynchronousChannel.configureSocketStatic(socket, socketSize);
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
        synchronized (this) {
            if (activeCount.get() > 0) {
                activeCount.decrementAndGet();
            }
        }
        finalizer.close();
        otherSocketAddress = null;
    }

    private static final class SocketSynchronousChannelFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private volatile DatagramChannel socketChannel;
        private volatile DatagramSocket socket;

        protected SocketSynchronousChannelFinalizer() {
            if (Throwables.isDebugStackTraceEnabled()) {
                initStackTrace = new Exception();
                initStackTrace.fillInStackTrace();
            } else {
                initStackTrace = null;
            }
        }

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
        protected void onRun() {
            String warning = "Finalizing unclosed " + DatagramSynchronousChannel.class.getSimpleName();
            if (Throwables.isDebugStackTraceEnabled()) {
                final Exception stackTrace = initStackTrace;
                if (stackTrace != null) {
                    warning += " from stacktrace:\n" + Throwables.getFullStackTrace(stackTrace);
                }
            }
            new Log(this).warn(warning);
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
