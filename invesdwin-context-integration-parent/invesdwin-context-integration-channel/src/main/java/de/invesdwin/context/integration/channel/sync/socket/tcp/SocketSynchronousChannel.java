package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.ABlockingDatagramSynchronousChannel;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class SocketSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile Socket socket;
    protected volatile SocketChannel socketChannel;
    protected volatile boolean socketChannelOpening;
    protected final SocketAddress socketAddress;
    protected final boolean server;
    protected volatile ServerSocketChannel serverSocketChannel;

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
        return socket;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public ServerSocketChannel getServerSocketChannel() {
        return serverSocketChannel;
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
                serverSocketChannel = newServerSocketChannel();
                serverSocketChannel.bind(socketAddress);
                socketChannel = serverSocketChannel.accept();
                try {
                    socket = socketChannel.socket();
                } catch (final Throwable t) {
                    //unix domain sockets throw an error here
                }
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                while (true) {
                    socketChannel = newSocketChannel();
                    try {
                        socketChannel.connect(socketAddress);
                        try {
                            socket = socketChannel.socket();
                        } catch (final Throwable t) {
                            //unix domain sockets throw an error here
                        }
                        break;
                    } catch (final IOException e) {
                        socketChannel.close();
                        socketChannel = null;
                        if (socket != null) {
                            socket.close();
                            socket = null;
                        }
                        if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                            try {
                                getMaxConnectRetryDelay().sleepRandom();
                            } catch (final InterruptedException e1) {
                                throw new RuntimeException(e1);
                            }
                        } else {
                            throw e;
                        }
                    }
                }
            }
            //non-blocking sockets are a bit faster than blocking ones
            socketChannel.configureBlocking(false);
            if (socket != null) {
                //might be unix domain socket
                socket.setTrafficClass(ABlockingDatagramSynchronousChannel.IPTOS_LOWDELAY
                        | ABlockingDatagramSynchronousChannel.IPTOS_THROUGHPUT);
                socket.setReceiveBufferSize(socketSize);
                socket.setSendBufferSize(socketSize);
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
            }
        } finally {
            socketChannelOpening = false;
        }
    }

    private void awaitSocketChannel() {
        try {
            //wait for channel
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while ((socketChannel == null || socketChannelOpening) && activeCount.get() > 0) {
                if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                    getWaitInterval().sleep();
                } else {
                    throw new ConnectException("Connection timeout");
                }
            }
        } catch (final Throwable t) {
            close();
            throw new RuntimeException(t);
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
        internalClose();
    }

    private void internalClose() {
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

}
