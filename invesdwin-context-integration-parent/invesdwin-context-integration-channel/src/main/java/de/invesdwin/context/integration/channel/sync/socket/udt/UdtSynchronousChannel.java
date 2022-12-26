package de.invesdwin.context.integration.channel.sync.socket.udt;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import com.barchart.udt.ExceptionUDT;
import com.barchart.udt.OptionUDT;
import com.barchart.udt.SocketUDT;
import com.barchart.udt.nio.SelectorProviderUDT;
import com.barchart.udt.nio.ServerSocketChannelUDT;
import com.barchart.udt.nio.SocketChannelUDT;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class UdtSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile boolean socketChannelOpening;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    private final SocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();

    public UdtSynchronousChannel(final InetSocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
        this.finalizer = new SocketSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public boolean isServer() {
        return server;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public SocketUDT getSocket() {
        return finalizer.socket;
    }

    public SocketChannelUDT getSocketChannel() {
        return finalizer.socketChannel;
    }

    public ServerSocketChannelUDT getServerSocketChannel() {
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
                //                SelectorProvider provider = SelectorProviderUDT.DATAGRAM;
                //                ServerSocketChannel acceptChannel = provider.openServerSocketChannel();
                //                ServerSocket acceptSocket = acceptChannel.socket();
                //                InetSocketAddress acceptAddress = new InetSocketAddress(&quot;localhost&quot;, 12345);
                //                acceptorSocket.bind(acceptAddress);
                //                assert acceptSocket.isBound();
                //                SocketChannel connectChannel = acceptChannel.accept();
                //                assert connectChannel.isConnected();

                finalizer.serverSocketChannel = newServerSocketChannel();
                configureSocket(finalizer.serverSocketChannel.socketUDT());
                finalizer.serverSocketChannel.socketUDT().bind(socketAddress);
                finalizer.serverSocketChannel.socketUDT().listen(256);
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                while (!finalizer.serverSocketChannel.socketUDT().isBound()) {
                    if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                        try {
                            getMaxConnectRetryDelay().sleepRandom();
                        } catch (final InterruptedException e1) {
                            throw new IOException(e1);
                        }
                    } else {
                        throw new IOException("bind timeout");
                    }
                }
                while (finalizer.socketChannel == null) {
                    finalizer.socketChannel = finalizer.serverSocketChannel.accept();
                    if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                        try {
                            getMaxConnectRetryDelay().sleepRandom();
                        } catch (final InterruptedException e1) {
                            throw new IOException(e1);
                        }
                    } else {
                        throw new IOException("accept timeout");
                    }
                }
                try {
                    finalizer.socket = finalizer.socketChannel.socketUDT();
                } catch (final Throwable t) {
                    //unix domain sockets throw an error here
                }
                configureSocket(finalizer.socket);
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                while (true) {
                    finalizer.socketChannel = newSocketChannel();
                    try {
                        configureSocket(finalizer.socketChannel.socketUDT());
                        while (!finalizer.socketChannel.socketUDT().isConnected()) {
                            finalizer.socketChannel.connect(socketAddress);
                            if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                                try {
                                    getMaxConnectRetryDelay().sleepRandom();
                                } catch (final InterruptedException e1) {
                                    throw new IOException(e1);
                                }
                            } else {
                                throw new IOException("connect timeout");
                            }
                        }
                        try {
                            finalizer.socket = finalizer.socketChannel.socketUDT();
                        } catch (final Throwable t) {
                            //unix domain sockets throw an error here
                        }
                        break;
                    } catch (final IOException e) {
                        finalizer.socketChannel.close();
                        finalizer.socketChannel = null;
                        final SocketUDT socket2 = finalizer.socket;
                        if (socket2 != null) {
                            socket2.close();
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
            //non-blocking sockets are a bit faster than blocking ones
            finalizer.socketChannel.configureBlocking(false);
        } finally {
            socketChannelOpening = false;
        }
    }

    private void configureSocket(final SocketUDT socket) throws ExceptionUDT {
        if (socket != null) {
            //might be unix domain socket
            //            socket.setReceiveBufferSize(Integers.max(socket.getReceiveBufferSize(), ByteBuffers.calculateExpansion(
            //                    socketSize * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER)));
            //            socket.setSendBufferSize(socketSize);
            socket.setOption(OptionUDT.UDT_SNDSYN, Boolean.FALSE);
            socket.setOption(OptionUDT.UDT_RCVSYN, Boolean.FALSE);
            socket.setOption(OptionUDT.Is_Receive_Synchronous, Boolean.FALSE);
            socket.setOption(OptionUDT.Is_Send_Synchronous, Boolean.FALSE);
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

    protected SocketChannelUDT newSocketChannel() throws IOException {
        return SelectorProviderUDT.DATAGRAM.openSocketChannel();
    }

    protected ServerSocketChannelUDT newServerSocketChannel() throws IOException {
        return SelectorProviderUDT.DATAGRAM.openServerSocketChannel();
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
        private volatile SocketUDT socket;
        private volatile SocketChannelUDT socketChannel;
        private volatile ServerSocketChannelUDT serverSocketChannel;

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
            final SocketUDT socketCopy = socket;
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
            String warning = "Finalizing unclosed " + UdtSynchronousChannel.class.getSimpleName();
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
