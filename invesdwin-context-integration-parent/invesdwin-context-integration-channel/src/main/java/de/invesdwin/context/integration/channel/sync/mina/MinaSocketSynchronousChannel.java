package de.invesdwin.context.integration.channel.sync.mina;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.mina.core.filterchain.IoFilterChain.Entry;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.ReadFuture;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.apr.AprSession;
import org.apache.mina.transport.socket.apr.AprSessionAccessor;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.Threads;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public class MinaSocketSynchronousChannel implements Closeable {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final IMinaSocketType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected final SocketAddress socketAddress;
    protected final boolean server;
    private volatile boolean sessionOpening;
    private final MinaSocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;

    private final IBufferingIterator<Consumer<IoSession>> sessionListeners = new BufferingIterator<>();
    private final AtomicInteger activeCount = new AtomicInteger();

    public MinaSocketSynchronousChannel(final IMinaSocketType type, final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
        this.finalizer = new MinaSocketSynchronousChannelFinalizer();
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

    public IMinaSocketType getType() {
        return type;
    }

    public IoSession getIoSession() {
        return finalizer.session;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public synchronized void addChannelListener(final Consumer<IoSession> channelListener) {
        if (channelListener != null) {
            sessionListeners.add(channelListener);
        }
    }

    public void open(final Consumer<IoSession> sessionListener, final boolean validateNative) throws IOException {
        if (!shouldOpen(sessionListener)) {
            awaitIoSession();
            return;
        }
        addChannelListener(sessionListener);
        if (server) {
            awaitSession(() -> {
                final IoAcceptor acceptor = type.newAcceptor();
                acceptor.setCloseOnDeactivation(false);
                acceptor.setHandler(new IoHandlerAdapter() {
                    @Override
                    public void sessionCreated(final IoSession session) throws Exception {
                        if (finalizer.session == null) {
                            onSession(session);
                            finalizer.session = session;
                            //only allow one client
                            if (type.isUnbindAcceptor()) {
                                acceptor.unbind();
                            }
                        } else {
                            //only allow one client
                            session.closeNow();
                        }
                    }
                });
                try {
                    acceptor.bind(socketAddress);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
                final long startNanos = System.nanoTime();
                while (finalizer.session == null) {
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
            });
        } else {
            final AtomicBoolean validatingConnect = new AtomicBoolean();
            awaitSession(() -> {
                final IoConnector connector = type.newConnector();
                connector.setHandler(new IoHandlerAdapter() {

                    @Override
                    public void exceptionCaught(final IoSession session, final Throwable cause) throws Exception {
                        if (!validatingConnect.get()) {
                            super.exceptionCaught(session, cause);
                        }
                    }

                    @Override
                    public void sessionOpened(final IoSession session) throws Exception {
                        onSession(session);
                        finalizer.session = session;
                    }
                });
                final ConnectFuture future = connector.connect(socketAddress);
                try {
                    future.await(getConnectTimeout().nanosValue(), TimeUnit.NANOSECONDS);
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Assertions.checkSame(finalizer.session, future.getSession());
                if (type.isValidateConnect()) {
                    validatingConnect.set(true);
                    finalizer.session.getConfig().setUseReadOperation(true);
                    try {
                        if (validateNative) {
                            final AprSession session = (AprSession) finalizer.session;
                            final long fd = AprSessionAccessor.getDescriptor(session);
                            //validate connection
                            final int count = Socket.recv(fd, Bytes.EMPTY_ARRAY, 0, 1);
                            if (count < 0 && !Status.APR_STATUS_IS_EAGAIN(-count)
                                    && !Status.APR_STATUS_IS_EOF(-count)) { // EOF
                                throw new RuntimeException(newTomcatException(count));
                            }
                        } else {
                            final ReadFuture readFuture = finalizer.session.read();
                            readFuture.await(getMaxConnectRetryDelay().nanosValue(), TimeUnit.NANOSECONDS);
                            final Object message = readFuture.getMessage();
                            if (message != null) {
                                final Entry filter = connector.getFilterChain().getAll().get(0);
                                filter.getFilter().messageReceived(filter.getNextFilter(), finalizer.session, message);
                            }
                        }
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        finalizer.session.getConfig().setUseReadOperation(false);
                        validatingConnect.set(false);
                    }
                }
            });
        }
    }

    private synchronized boolean shouldOpen(final Consumer<IoSession> channelListener) {
        if (activeCount.incrementAndGet() > 1) {
            if (channelListener != null) {
                channelListener.accept(finalizer.session);
            }
            return false;
        } else {
            return true;
        }
    }

    /**
     * Can be overridden to add handlers
     */
    protected void onSession(final IoSession session) {
        triggerSessionListeners(session);
        if (!isWriterRegistered()) {
            session.suspendWrite();
        }
        if (!isReaderRegistered()) {
            session.suspendRead();
        }
    }

    private void triggerSessionListeners(final IoSession channel) {
        try {
            while (true) {
                final Consumer<IoSession> next = sessionListeners.next();
                next.accept(channel);
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
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
        awaitIoSession();
    }

    private void awaitIoSession() throws IOException {
        try {
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            //wait for channel
            while ((finalizer.session == null || sessionOpening) && activeCount.get() > 0) {
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
        finalizer.closeIoSession();
        final IoAcceptor serverAcceptorCopy = finalizer.serverAcceptor;
        if (serverAcceptorCopy != null) {
            finalizer.serverAcceptor = null;
            serverAcceptorCopy.unbind();
            serverAcceptorCopy.dispose();
        }
        final IoConnector clientConnectorCopy = finalizer.clientConnector;
        if (clientConnectorCopy != null) {
            finalizer.clientConnector = null;
            clientConnectorCopy.dispose();
        }
    }

    public void closeAsync() {
        synchronized (this) {
            if (activeCount.get() > 0) {
                activeCount.decrementAndGet();
            }
        }
        finalizer.close();
    }

    public void closeBootstrapAsync() {
        finalizer.closeBootstrapAsync();
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

    private static final class MinaSocketSynchronousChannelFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private volatile IoSession session;
        private volatile IoAcceptor serverAcceptor;
        private volatile IoConnector clientConnector;

        protected MinaSocketSynchronousChannelFinalizer() {
            if (Throwables.isDebugStackTraceEnabled()) {
                initStackTrace = new Exception();
                initStackTrace.fillInStackTrace();
            } else {
                initStackTrace = null;
            }
        }

        @Override
        protected void clean() {
            closeIoSession();
            closeBootstrapAsync();
        }

        @Override
        protected void onRun() {
            String warning = "Finalizing unclosed " + MinaSocketSynchronousChannel.class.getSimpleName();
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
            return session == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

        public void closeIoSession() {
            final IoSession sessionCopy = session;
            if (sessionCopy != null) {
                session = null;
                sessionCopy.closeOnFlush();
            }
        }

        public void closeBootstrapAsync() {
            if (serverAcceptor != null) {
                serverAcceptor.unbind();
                serverAcceptor.dispose();
                serverAcceptor = null;
            }
            if (clientConnector != null) {
                clientConnector.dispose();
                clientConnector = null;
            }
        }

    }

    public static IOException newTomcatException(final int code) {
        return new IOException(org.apache.tomcat.jni.Error.strerror(-code) + " (code: " + code + ")");
    }

}
