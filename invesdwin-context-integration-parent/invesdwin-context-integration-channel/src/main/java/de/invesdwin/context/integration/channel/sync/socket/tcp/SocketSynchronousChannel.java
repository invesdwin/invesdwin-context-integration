package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BlockingSocketSynchronousChannel;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class SocketSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile boolean socketChannelOpening;
    protected final SocketAddress socketAddress;
    protected final boolean server;
    private final SocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();

    public SocketSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
        this.finalizer = new SocketSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    SocketSynchronousChannel(final SocketSynchronousChannelServer server, final SocketChannel socketChannel)
            throws IOException {
        this.socketAddress = server.socketAddress;
        this.server = false;
        this.estimatedMaxMessageSize = server.getEstimatedMaxMessageSize();
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
        this.finalizer = new SocketSynchronousChannelFinalizer();
        finalizer.socketChannel = socketChannel;
        finalizer.socket = extractSocket(socketChannel);
        finalizer.register(this);
        activeCount.incrementAndGet();
        configure();
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public boolean isServer() {
        return server;
    }

    public int getEstimatedMaxMessageSize() {
        return estimatedMaxMessageSize;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public Socket getSocket() {
        return finalizer.socket;
    }

    public SocketChannel getSocketChannel() {
        return finalizer.socketChannel;
    }

    public ServerSocketChannel getServerSocketChannel() {
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
                finalizer.serverSocketChannel = newServerSocketChannel();
                finalizer.serverSocketChannel.bind(socketAddress);
                finalizer.socketChannel = finalizer.serverSocketChannel.accept();
                try {
                    finalizer.socket = finalizer.socketChannel.socket();
                } catch (final Throwable t) {
                    //unix domain sockets throw an error here
                }
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                while (true) {
                    finalizer.socketChannel = newSocketChannel();
                    try {
                        finalizer.socketChannel.connect(socketAddress);
                        finalizer.socket = extractSocket(finalizer.socketChannel);
                        break;
                    } catch (final IOException e) {
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
            configure();
        } finally {
            socketChannelOpening = false;
        }
    }

    private void configure() throws IOException {
        configureSocketChannel();
        if (finalizer.socket != null) {
            configureSocket(finalizer.socket);
        }
    }

    protected void configureSocketChannel() throws IOException {
        //non-blocking sockets are a bit faster than blocking ones
        finalizer.socketChannel.configureBlocking(false);
    }

    private static Socket extractSocket(final SocketChannel socketChannel) {
        try {
            return socketChannel.socket();
        } catch (final Throwable t) {
            //unix domain sockets throw an error here
            return null;
        }
    }

    protected void configureSocket(final Socket socket) throws SocketException {
        BlockingSocketSynchronousChannel.configureSocketStatic(socket, socketSize);
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
    }

    private static final class SocketSynchronousChannelFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private volatile Socket socket;
        private volatile SocketChannel socketChannel;
        private volatile ServerSocketChannel serverSocketChannel;

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
            final SocketChannel socketChannelCopy = socketChannel;
            if (socketChannelCopy != null) {
                socketChannel = null;
                Closeables.closeQuietly(socketChannelCopy);
            }
            final Socket socketCopy = socket;
            if (socketCopy != null) {
                socket = null;
                Closeables.closeQuietly(socketCopy);
            }
            final ServerSocketChannel serverSocketChannelCopy = serverSocketChannel;
            if (serverSocketChannelCopy != null) {
                serverSocketChannel = null;
                Closeables.closeQuietly(serverSocketChannelCopy);
            }
        }

        @Override
        protected void onRun() {
            String warning = "Finalizing unclosed " + SocketSynchronousChannel.class.getSimpleName();
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
