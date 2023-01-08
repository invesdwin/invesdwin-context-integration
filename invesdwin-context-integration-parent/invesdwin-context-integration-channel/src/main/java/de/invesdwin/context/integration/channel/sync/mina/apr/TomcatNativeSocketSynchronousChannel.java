package de.invesdwin.context.integration.channel.sync.mina.apr;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.transport.socket.apr.AprLibraryAccessor;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Error;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.concurrent.Threads;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.time.duration.Duration;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

@ThreadSafe
public class TomcatNativeSocketSynchronousChannel implements Closeable {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    /**
     * This constant is deduced from the APR code. It is used when the timeout has expired while doing a poll()
     * operation.
     */
    private static final int APR_TIMEUP_ERROR = -120001;
    private static final int POLLSET_SIZE = 1024;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    private volatile boolean sessionOpening;
    private final NettySocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;

    private final AtomicInteger activeCount = new AtomicInteger();

    public TomcatNativeSocketSynchronousChannel(final InetSocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
        this.finalizer = new NettySocketSynchronousChannelFinalizer();
        finalizer.register(this);
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
                openServer();
            });
        } else {
            awaitSession(() -> {
                openClient();
            });
        }
    }

    private void openServer() {
        try {
            final InetSocketAddress la = socketAddress;
            final long acceptorHandle = Socket.create(Socket.APR_INET, Socket.SOCK_STREAM, Socket.APR_PROTO_TCP,
                    finalizer.pool);
            long pollset = 0;

            try {
                openAcceptor(la, acceptorHandle);

                pollset = openPollset();

                final int result = Poll.add(pollset, acceptorHandle, Poll.APR_POLLIN);
                if (result != Status.APR_SUCCESS) {
                    throw newTomcatException(result);
                }

                final long[] polledSockets = new long[POLLSET_SIZE << 1];
                final LongList polledHandles = new LongArrayList();

                final long startNanos = System.nanoTime();
                while (polledHandles.isEmpty()) {
                    select(pollset, polledSockets, polledHandles);
                    if (polledHandles.isEmpty()) {
                        if (Threads.isInterrupted()
                                || getConnectTimeout().isLessThanNanos(System.nanoTime() - startNanos)) {
                            throw new RuntimeException("timeout exceeded");
                        }
                        try {
                            getMaxConnectRetryDelay().sleepRandom();
                        } catch (final InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                //allow only one connection
                for (int i = 1; i < polledHandles.size(); i++) {
                    closeHandle(polledHandles.get(i));
                }
                finalizer.fd = polledHandles.getLong(0);
            } finally {
                //unbind
                closeHandle(acceptorHandle);
                if (pollset > 0) {
                    Poll.destroy(pollset);
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void openAcceptor(final InetSocketAddress la, final long acceptorHandle)
            throws IOException, Exception, Error {
        int result = Socket.optSet(acceptorHandle, Socket.APR_SO_NONBLOCK, 1);
        if (result != Status.APR_SUCCESS) {
            throw newTomcatException(result);
        }
        result = Socket.timeoutSet(acceptorHandle, 0);
        if (result != Status.APR_SUCCESS) {
            throw newTomcatException(result);
        }

        // Configure the server socket,
        result = Socket.optSet(acceptorHandle, Socket.APR_SO_REUSEADDR, 0);
        if (result != Status.APR_SUCCESS) {
            throw newTomcatException(result);
        }
        result = Socket.optSet(acceptorHandle, Socket.APR_SO_RCVBUF,
                Integers.max(Socket.optGet(acceptorHandle, Socket.APR_SO_RCVBUF), ByteBuffers.calculateExpansion(
                        socketSize * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER)));
        if (result != Status.APR_SUCCESS) {
            throw newTomcatException(result);
        }

        // and bind.
        final long sa;
        if (la != null) {
            if (la.getAddress() == null) {
                sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, la.getPort(), 0, finalizer.pool);
            } else {
                sa = Address.info(la.getAddress().getHostAddress(), Socket.APR_INET, la.getPort(), 0, finalizer.pool);
            }
        } else {
            sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, 0, 0, finalizer.pool);
        }

        result = Socket.bind(acceptorHandle, sa);
        if (result != Status.APR_SUCCESS) {
            throw newTomcatException(result);
        }
        result = Socket.listen(acceptorHandle, 1);
        if (result != Status.APR_SUCCESS) {
            throw newTomcatException(result);
        }
    }

    private long openPollset() throws Error {
        long pollset;
        pollset = Poll.create(POLLSET_SIZE, finalizer.pool, Poll.APR_POLLSET_THREADSAFE, Long.MAX_VALUE);

        if (pollset <= 0) {
            pollset = Poll.create(62, finalizer.pool, Poll.APR_POLLSET_THREADSAFE, Long.MAX_VALUE);
        }

        if (pollset <= 0) {
            if (Status.APR_STATUS_IS_ENOTIMPL(-(int) pollset)) {
                throw new RuntimeIoException("Thread-safe pollset is not supported in this platform.");
            }
        }
        return pollset;
    }

    protected int select(final long pollset, final long[] polledSockets, final LongList polledHandles)
            throws Exception {
        int rv = Poll.poll(pollset, Integer.MAX_VALUE, polledSockets, false);
        if (rv <= 0) {
            // We have had an error. It can simply be that we have reached
            // the timeout (very unlikely, as we have set it to MAX_INTEGER)
            if (rv != APR_TIMEUP_ERROR) {
                // It's not a timeout being exceeded. Throw the error
                throw newTomcatException(rv);
            }

            rv = Poll.maintain(pollset, polledSockets, true);
            if (rv > 0) {
                for (int i = 0; i < rv; i++) {
                    Poll.add(pollset, polledSockets[i], Poll.APR_POLLIN);
                }
            } else if (rv < 0) {
                throw newTomcatException(rv);
            }

            return 0;
        } else {
            rv <<= 1;
            if (!polledHandles.isEmpty()) {
                polledHandles.clear();
            }

            for (int i = 0; i < rv; i++) {
                final long flag = polledSockets[i];
                final long socket = polledSockets[++i];

                if ((flag & Poll.APR_POLLIN) != 0) {
                    polledHandles.add(socket);
                }
            }
            return polledHandles.size();
        }
    }

    private void openClient() {
        try {
            final long handle = Socket.create(Socket.APR_INET, Socket.SOCK_STREAM, Socket.APR_PROTO_TCP,
                    finalizer.pool);
            boolean success = false;

            try {
                int result = Socket.optSet(handle, Socket.APR_SO_NONBLOCK, 1);
                if (result != Status.APR_SUCCESS) {
                    throw newTomcatException(result);
                }
                result = Socket.timeoutSet(handle, 0);
                if (result != Status.APR_SUCCESS) {
                    throw newTomcatException(result);
                }

                if (socketAddress != null) {
                    final InetSocketAddress la = socketAddress;
                    final long sa;

                    if (la.getAddress() == null) {
                        sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, la.getPort(), 0, finalizer.pool);
                    } else {
                        sa = Address.info(la.getAddress().getHostAddress(), Socket.APR_INET, la.getPort(), 0,
                                finalizer.pool);
                    }

                    result = Socket.bind(handle, sa);
                    if (result != Status.APR_SUCCESS) {
                        throw newTomcatException(result);
                    }
                }

                success = true;
                finalizer.fd = handle;
            } finally {
                if (!success) {
                    closeHandle(handle);
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        try {
            //validate connection
            final int count = Socket.recv(finalizer.fd, Bytes.EMPTY_ARRAY, 0, 1);
            if (count < 0 && !Status.APR_STATUS_IS_EAGAIN(-count) && !Status.APR_STATUS_IS_EOF(-count)) { // EOF
                throw new RuntimeException(TomcatNativeSocketSynchronousChannel.newTomcatException(count));
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void closeHandle(final long handle) throws IOException {
        final int rv = Socket.close(handle);
        if (rv != Status.APR_SUCCESS) {
            throw newTomcatException(rv);
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
        synchronized (this) {
            if (activeCount.get() > 0) {
                activeCount.decrementAndGet();
            }
        }
        internalClose();
        finalizer.close();
    }

    private void internalClose() {
        finalizer.clean();
    }

    public void closeAsync() {
        synchronized (this) {
            if (activeCount.get() > 0) {
                activeCount.decrementAndGet();
            }
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

    private static final class NettySocketSynchronousChannelFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private volatile long pool;
        private volatile long fd;

        protected NettySocketSynchronousChannelFinalizer() {
            if (Throwables.isDebugStackTraceEnabled()) {
                initStackTrace = new Exception();
                initStackTrace.fillInStackTrace();
            } else {
                initStackTrace = null;
            }
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
        protected void onRun() {
            String warning = "Finalizing unclosed " + TomcatNativeSocketSynchronousChannel.class.getSimpleName();
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
            return fd == 0;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

    public static IOException newTomcatException(final int code) {
        return new IOException(org.apache.tomcat.jni.Error.strerror(-code) + " (code: " + code + ")");
    }

}
