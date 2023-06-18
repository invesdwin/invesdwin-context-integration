package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.netty.SelectStrategyFactories;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.socket.SocketChannel;

@ThreadSafe
public class NettySocketClientEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {

    private final INettySocketChannelType type;
    private final InetSocketAddress socketAddress;
    private final int socketSize;
    private final NettySocketClientEndpointFactoryFinalizer bootstrapFinalizer;
    private final AtomicInteger bootstrapActiveCount = new AtomicInteger();
    @GuardedBy("self")
    private final IBufferingIterator<NettySocketClientEndpointChannel> connectQueue = new BufferingIterator<>();

    public NettySocketClientEndpointFactory(final INettySocketChannelType type, final InetSocketAddress socketAddress,
            final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.socketSize = estimatedMaxMessageSize + NettySocketSynchronousChannel.MESSAGE_INDEX;

        this.bootstrapFinalizer = new NettySocketClientEndpointFactoryFinalizer();
        bootstrapFinalizer.register(this);
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final NettySocketSynchronousChannel connector = new NettySocketClientEndpointChannel(type, socketAddress, false,
                socketSize);
        final NettySocketSynchronousReader reader = new NettySocketSynchronousReader(connector);
        final NettySocketSynchronousWriter writer = new NettySocketSynchronousWriter(connector);
        return ImmutableSynchronousEndpoint.of(reader, writer);
    }

    private void maybeInitClientBootstrap() {
        if (bootstrapFinalizer.clientBootstrap == null) {
            synchronized (this) {
                if (bootstrapFinalizer.clientBootstrap == null) {
                    bootstrapFinalizer.clientBootstrap = new Bootstrap();
                    bootstrapFinalizer.clientBootstrap.group(type.newClientWorkerGroup(
                            newClientWorkerGroupThreadCount(), newClientWorkerGroupSelectStrategyFactory()));
                    bootstrapFinalizer.clientBootstrap.channel(type.getClientChannelType());
                    type.channelOptions(bootstrapFinalizer.clientBootstrap::option, socketSize, false);
                    bootstrapFinalizer.clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(final SocketChannel ch) throws Exception {
                            final NettySocketClientEndpointChannel connector;
                            synchronized (connectQueue) {
                                if (connectQueue.hasNext()) {
                                    connector = connectQueue.next();
                                } else {
                                    connector = null;
                                }
                            }
                            if (connector != null) {
                                connector.onConnected(ch);
                            } else {
                                //not requested
                                ch.close();
                            }
                        }

                    });
                }
            }
        }
    }

    /**
     * Can be overridden to add handlers
     */
    protected void onSocketChannel(final SocketChannel socketChannel) {}

    protected SelectStrategyFactory newClientWorkerGroupSelectStrategyFactory() {
        return SelectStrategyFactories.DEFAULT;
    }

    protected int newClientWorkerGroupThreadCount() {
        return 1;
    }

    private final class NettySocketClientEndpointChannel extends NettySocketSynchronousChannel {

        @GuardedBy("this")
        private boolean opened;

        private NettySocketClientEndpointChannel(final INettySocketChannelType type,
                final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
            super(type, socketAddress, server, estimatedMaxMessageSize);
        }

        @Override
        protected SelectStrategyFactory newClientWorkerGroupSelectStrategyFactory() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int newClientWorkerGroupThreadCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int newServerAcceptorGroupThreadCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected SelectStrategyFactory newServerWorkerGroupSelectStrategyFactory() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int newServerWorkerGroupThreadCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void open(final Consumer<SocketChannel> channelListener) throws IOException {
            super.open(channelListener);
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
        protected void connect() throws IOException {
            synchronized (connectQueue) {
                connectQueue.add(this);
            }
            maybeInitClientBootstrap();
            awaitSocketChannel(() -> {
                return bootstrapFinalizer.clientBootstrap.connect(socketAddress);
            });
        }

        void onConnected(final SocketChannel ch) {
            type.initChannel(ch, false);
            onSocketChannel(ch);
            finalizer.socketChannel = ch;
        }

        @Override
        protected void onSocketChannel(final SocketChannel socketChannel) {
            NettySocketClientEndpointFactory.this.onSocketChannel(socketChannel);
            super.onSocketChannel(socketChannel);
        }

    }

    private static final class NettySocketClientEndpointFactoryFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private volatile Bootstrap clientBootstrap;

        protected NettySocketClientEndpointFactoryFinalizer() {
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
        }

        @Override
        protected void onRun() {
            String warning = "Finalizing unclosed " + NettySocketSynchronousChannel.class.getSimpleName();
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
            return clientBootstrap == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

        public void closeBootstrapAsync() {
            if (clientBootstrap != null) {
                final BootstrapConfig config = clientBootstrap.config();
                clientBootstrap = null;
                final EventLoopGroup group = config.group();
                NettySocketSynchronousChannel.shutdownGracefully(group);
            }
        }

    }

}
