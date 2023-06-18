package de.invesdwin.context.integration.channel.sync.mina;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.math.Booleans;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import io.netty.util.concurrent.FastThreadLocal;

@ThreadSafe
public class MinaSocketClientEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {

    private static final FastThreadLocal<Boolean> VALIDATING_CONNECT = new FastThreadLocal<>();

    private final IMinaSocketType type;
    private final InetSocketAddress socketAddress;
    private final int socketSize;
    private final MinaSocketClientEndpointFactoryFinalizer bootstrapFinalizer;
    private final AtomicInteger bootstrapActiveCount = new AtomicInteger();
    @GuardedBy("self")
    private final IBufferingIterator<MinaSocketClientEndpointChannel> connectQueue = new BufferingIterator<>();

    public MinaSocketClientEndpointFactory(final IMinaSocketType type, final InetSocketAddress socketAddress,
            final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.socketSize = estimatedMaxMessageSize + MinaSocketSynchronousChannel.MESSAGE_INDEX;

        this.bootstrapFinalizer = new MinaSocketClientEndpointFactoryFinalizer();
        bootstrapFinalizer.register(this);
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final MinaSocketSynchronousChannel connector = new MinaSocketClientEndpointChannel(type, socketAddress, false,
                socketSize);
        final MinaSocketSynchronousReader reader = new MinaSocketSynchronousReader(connector);
        final MinaSocketSynchronousWriter writer = new MinaSocketSynchronousWriter(connector);
        return ImmutableSynchronousEndpoint.of(reader, writer);
    }

    private void maybeInitClientConnector() {
        if (bootstrapFinalizer.clientConnector == null) {
            synchronized (this) {
                if (bootstrapFinalizer.clientConnector == null) {
                    bootstrapFinalizer.executor = newConnectorExecutor();
                    bootstrapFinalizer.clientConnector = type.newConnector(bootstrapFinalizer.executor,
                            newConnectorProcessorCount());
                    bootstrapFinalizer.clientConnector.setHandler(new IoHandlerAdapter() {

                        @Override
                        public void exceptionCaught(final IoSession session, final Throwable cause) throws Exception {
                            if (Booleans.isNotTrue(VALIDATING_CONNECT.get())) {
                                super.exceptionCaught(session, cause);
                            }
                        }

                        @Override
                        public void sessionOpened(final IoSession session) throws Exception {
                            final MinaSocketClientEndpointChannel connector;
                            synchronized (connectQueue) {
                                if (connectQueue.hasNext()) {
                                    connector = connectQueue.next();
                                } else {
                                    connector = null;
                                }
                            }
                            if (connector != null) {
                                connector.onConnected(session);
                            } else {
                                //not requested
                                session.closeNow();
                            }
                        }
                    });
                }
            }
        }
    }

    protected int newConnectorProcessorCount() {
        return 1;
    }

    protected ExecutorService newConnectorExecutor() {
        //keep default of mina
        return null;
    }

    /**
     * Can be overridden to add handlers
     */
    protected void onSession(final IoSession session) {}

    private final class MinaSocketClientEndpointChannel extends MinaSocketSynchronousChannel {

        @GuardedBy("this")
        private boolean opened;

        private MinaSocketClientEndpointChannel(final IMinaSocketType type, final InetSocketAddress socketAddress,
                final boolean server, final int estimatedMaxMessageSize) {
            super(type, socketAddress, server, estimatedMaxMessageSize);
        }

        @Override
        protected java.util.concurrent.ExecutorService newAcceptorExecutor() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int newAcceptorProcessorCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected java.util.concurrent.ExecutorService newConnectorExecutor() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int newConnectorProcessorCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void open(final Consumer<IoSession> sessionListener, final boolean validateNative) throws IOException {
            super.open(sessionListener, validateNative);
            synchronized (this) {
                if (!opened) {
                    bootstrapActiveCount.incrementAndGet();
                    opened = true;
                }
            }
        }

        @Override
        public void close() {
            super.close();
            boolean close = false;
            synchronized (this) {
                if (!opened) {
                    return;
                }
                if (bootstrapActiveCount.get() > 0) {
                    final int newCount = bootstrapActiveCount.decrementAndGet();
                    if (newCount == 0) {
                        close = true;
                    }
                }
                opened = false;
            }
            if (close) {
                bootstrapFinalizer.close();
            }
        }

        @Override
        protected void connect(final boolean validateNative) throws IOException {
            synchronized (connectQueue) {
                connectQueue.add(this);
            }
            maybeInitClientConnector();
            awaitSession(() -> {
                final ConnectFuture future = bootstrapFinalizer.clientConnector.connect(socketAddress);
                try {
                    future.await(getConnectTimeout().nanosValue(), TimeUnit.NANOSECONDS);
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Assertions.checkSame(finalizer.session, future.getSession());
                if (type.isValidateConnect()) {
                    VALIDATING_CONNECT.set(true);
                    finalizer.session.getConfig().setUseReadOperation(true);
                    try {
                        validateConnect(validateNative);
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        finalizer.session.getConfig().setUseReadOperation(false);
                        VALIDATING_CONNECT.remove();
                    }
                }
            });
        }

        void onConnected(final IoSession session) {
            onSession(session);
            finalizer.session = session;
        }

        @Override
        protected void onSession(final IoSession session) {
            MinaSocketClientEndpointFactory.this.onSession(session);
            super.onSession(session);
        }

    }

    private static final class MinaSocketClientEndpointFactoryFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private volatile ExecutorService executor;
        private volatile IoConnector clientConnector;

        protected MinaSocketClientEndpointFactoryFinalizer() {
            if (Throwables.isDebugStackTraceEnabled()) {
                initStackTrace = new Exception();
                initStackTrace.fillInStackTrace();
            } else {
                initStackTrace = null;
            }
        }

        @Override
        protected void clean() {
            closeBootstrapAsync();
            if (executor != null) {
                executor.shutdownNow();
                executor = null;
            }
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
            return clientConnector == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

        public void closeBootstrapAsync() {
            if (clientConnector != null) {
                clientConnector.dispose();
                clientConnector = null;
            }
        }

    }

}
