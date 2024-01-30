package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.io.IOException;
import java.net.ConnectException;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class BlockingDatagramSynchronousChannel implements ISessionlessSynchronousChannel<SocketAddress> {

    public static final int IPTOS_LOWCOST = 0x02;
    public static final int IPTOS_RELIABILITY = 0x04;
    public static final int IPTOS_THROUGHPUT = 0x08;
    public static final int IPTOS_LOWDELAY = 0x10;
    public static final int RECEIVE_BUFFER_SIZE_MULTIPLIER = 10;

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile boolean socketOpening;
    protected final SocketAddress socketAddress;
    protected SocketAddress otherSocketAddress;
    protected final boolean server;
    private final SocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();
    private boolean multipleClientsAllowed;

    public BlockingDatagramSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
        this.finalizer = new SocketSynchronousChannelFinalizer();
        finalizer.register(this);
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

    public int getSocketSize() {
        return socketSize;
    }

    public DatagramSocket getSocket() {
        return finalizer.socket;
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
        socketOpening = true;
        try {
            if (server) {
                finalizer.socket = new DatagramSocket(socketAddress);
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                while (true) {
                    try {
                        finalizer.socket = new DatagramSocket();
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
            configureSocket(finalizer.socket);
        } finally {
            socketOpening = false;
        }
    }

    protected void configureSocket(final DatagramSocket socket) throws SocketException {
        configureSocketStatic(socket, socketSize);
    }

    public static void configureSocketStatic(final DatagramSocket socket, final int socketSize) throws SocketException {
        socket.setSendBufferSize(socketSize);
        socket.setReceiveBufferSize(Integers.max(socket.getReceiveBufferSize(),
                ByteBuffers.calculateExpansion(socketSize * RECEIVE_BUFFER_SIZE_MULTIPLIER)));
        socket.setTrafficClass(BlockingDatagramSynchronousChannel.IPTOS_LOWDELAY
                | BlockingDatagramSynchronousChannel.IPTOS_THROUGHPUT);
    }

    private void awaitSocketChannel() throws IOException {
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
        otherSocketAddress = null;
    }

    private static final class SocketSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile DatagramSocket socket;

        @Override
        protected void clean() {
            if (socket != null) {
                socket.close();
                socket = null;
            }
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
