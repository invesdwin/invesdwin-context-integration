package de.invesdwin.context.integration.channel.sync.mina.apr;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.mina.transport.socket.apr.AprLibraryAccessor;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public class MinaNativeDatagramSynchronousChannel implements Closeable {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    private volatile boolean sessionOpening;
    private final MinaSocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;

    private final AtomicInteger activeCount = new AtomicInteger();

    public MinaNativeDatagramSynchronousChannel(final InetSocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = newSocketSize(estimatedMaxMessageSize);
        this.finalizer = new MinaSocketSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    protected int newSocketSize(final int estimatedMaxMessageSize) {
        return estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    public boolean isServer() {
        return server;
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

    public long getFileDescriptor() {
        return finalizer.fd;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public void open() throws IOException {
        if (!shouldOpen()) {
            awaitFileDescriptor();
            return;
        }

        finalizer.pool = Pool.create(AprLibraryAccessor.getRootPool());

        if (server) {
            awaitSession(() -> {
                try {
                    MinaNativeDatagramServerOpener.openServer(MinaNativeDatagramSynchronousChannel.this);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            awaitSession(() -> {
                try {
                    MinaNativeDatagramClientOpener.openClient(MinaNativeDatagramSynchronousChannel.this);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        init();

        Socket.optSet(finalizer.getFd(), Socket.APR_SO_NONBLOCK, 1);
        Socket.timeoutSet(finalizer.getFd(), 0);
    }

    private void init() {
        // initialize a memory pool for APR functions
        finalizer.pool = Pool.create(AprLibraryAccessor.getRootPool());
    }

    public static void closeHandle(final long handle) throws IOException {
        final int rv = Socket.close(handle);
        if (rv != Status.APR_SUCCESS) {
            throw MinaSocketSynchronousChannel.newTomcatException(rv);
        }
    }

    private synchronized boolean shouldOpen() {
        return activeCount.incrementAndGet() <= 1;
    }

    private void awaitSession(final Runnable sessionFactory) throws IOException {
        sessionOpening = true;
        try {
            //init bootstrap
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while (activeCount.get() > 0) {
                try {
                    sessionFactory.run();
                    break;
                } catch (final Throwable t) {
                    if (activeCount.get() > 0) {
                        internalClose();
                        if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                            try {
                                getMaxConnectRetryDelay().sleepRandom();
                            } catch (final InterruptedException e1) {
                                throw new IOException(e1);
                            }
                        } else {
                            throw t;
                        }
                    } else {
                        return;
                    }
                }
            }
        } catch (final Throwable t) {
            closeAsync();
            throw new IOException(t);
        } finally {
            sessionOpening = false;
        }
        awaitFileDescriptor();
    }

    private void awaitFileDescriptor() throws IOException {
        try {
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            //wait for channel
            while ((finalizer.fd == 0 || sessionOpening) && activeCount.get() > 0) {
                if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                    getWaitInterval().sleep();
                } else {
                    throw new ConnectException("Connection timeout");
                }
            }
        } catch (final Throwable t) {
            closeAsync();
            throw new IOException(t);
        }
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
        internalClose();
        finalizer.close();
    }

    private synchronized boolean shouldClose() {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        return activeCountBefore == 1;
    }

    private void internalClose() {
        finalizer.clean();
    }

    public void closeAsync() {
        if (!shouldClose()) {
            return;
        }
        finalizer.close();
    }

    public static void awaitShutdown(final Future<?> future) {
        if (future == null) {
            return;
        }
        try {
            Futures.get(future);
        } catch (final Throwable t) {
            //ignore
        }
    }

    MinaSocketSynchronousChannelFinalizer getFinalizer() {
        return finalizer;
    }

    public static final class MinaSocketSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile long pool;
        private volatile long fd;

        public long getFd() {
            return fd;
        }

        public void setFd(final long fd) {
            this.fd = fd;
        }

        public long getPool() {
            return pool;
        }

        public void setPool(final long pool) {
            this.pool = pool;
        }

        @Override
        protected void clean() {
            if (pool > 0) {
                Pool.destroy(pool);
                pool = 0;
            }
            if (fd > 0) {
                try {
                    closeHandle(fd);
                } catch (final IOException e) {
                    //ignore
                }
                fd = 0;
            }
        }

        @Override
        protected boolean isCleaned() {
            return fd == 0;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

}
