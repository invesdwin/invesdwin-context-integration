package de.invesdwin.context.integration.channel.sync.socket.tcp.blocking;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
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
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class BlockingSocketSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile boolean socketOpening;
    protected final SocketAddress socketAddress;
    protected final boolean server;
    protected final SocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();

    public BlockingSocketSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
        this.finalizer = new SocketSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public boolean isServer() {
        return server;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public Socket getSocket() {
        return finalizer.socket;
    }

    public ServerSocket getServerSocket() {
        return finalizer.serverSocket;
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

    public boolean isInputStreamAvailableSupported() {
        return true;
    }

    @Override
    public void open() throws IOException {
        if (!shouldOpen()) {
            awaitSocket();
            return;
        }
        socketOpening = true;
        try {
            internalOpen();
        } finally {
            socketOpening = false;
        }
    }

    protected void internalOpen() throws IOException {
        if (server) {
            finalizer.serverSocket = new ServerSocket();
            finalizer.serverSocket.bind(socketAddress);
            finalizer.socket = finalizer.serverSocket.accept();
        } else {
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while (true) {
                try {
                    finalizer.socket = new Socket();
                    finalizer.socket.connect(socketAddress);
                    break;
                } catch (final ConnectException e) {
                    finalizer.socket.close();
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
        finalizer.socket.setTrafficClass(BlockingDatagramSynchronousChannel.IPTOS_LOWDELAY
                | BlockingDatagramSynchronousChannel.IPTOS_THROUGHPUT);
        finalizer.socket.setReceiveBufferSize(Integers.max(finalizer.socket.getReceiveBufferSize(), ByteBuffers
                .calculateExpansion(socketSize * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER)));
        finalizer.socket.setSendBufferSize(socketSize);
        finalizer.socket.setTcpNoDelay(true);
        finalizer.socket.setKeepAlive(true);
    }

    private void awaitSocket() throws IOException {
        try {
            //wait for channel
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while ((finalizer.socket == null || socketOpening) && activeCount.get() > 0) {
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

    protected static final class SocketSynchronousChannelFinalizer extends AFinalizer {

        protected volatile Socket socket;
        protected volatile ServerSocket serverSocket;
        private final Exception initStackTrace;

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
            final Socket socketCopy = socket;
            if (socketCopy != null) {
                socket = null;
                Closeables.closeQuietly(socketCopy);
            }
            final ServerSocket serverSocketCopy = serverSocket;
            if (serverSocketCopy != null) {
                serverSocket = null;
                Closeables.closeQuietly(serverSocketCopy);
            }
        }

        @Override
        protected void onRun() {
            String warning = "Finalizing unclosed " + BlockingSocketSynchronousChannel.class.getSimpleName();
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
            return socket == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

}
