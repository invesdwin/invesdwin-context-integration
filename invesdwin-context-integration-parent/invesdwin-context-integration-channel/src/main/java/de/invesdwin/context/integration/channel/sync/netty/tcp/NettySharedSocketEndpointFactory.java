package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.netty.SelectStrategyFactories;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.socket.SocketChannel;

@ThreadSafe
public class NettySharedSocketEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {

    private final INettySocketChannelType type;
    private final InetSocketAddress socketAddress;
    private final int socketSize;
    private final boolean lowLatency;
    private final NettySharedSocketEndpointFactoryFinalizer bootstrapFinalizer;
    private final AtomicInteger bootstrapActiveCount = new AtomicInteger();
    @GuardedBy("self")
    private final IBufferingIterator<NettySocketClientEndpointChannel> connectQueue = new BufferingIterator<>();

    public NettySharedSocketEndpointFactory(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final int estimatedMaxMessageSize, final boolean lowLatency) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.socketSize = newSocketSize(estimatedMaxMessageSize);
        this.lowLatency = lowLatency;

        this.bootstrapFinalizer = new NettySharedSocketEndpointFactoryFinalizer();
        bootstrapFinalizer.register(this);
    }

    protected int newSocketSize(final int estimatedMaxMessageSize) {
        return estimatedMaxMessageSize + NettySocketSynchronousChannel.MESSAGE_INDEX;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final NettySocketSynchronousChannel connector = new NettySocketClientEndpointChannel(type, socketAddress, false,
                socketSize, lowLatency);
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
                    type.channelOptions(bootstrapFinalizer.clientBootstrap::option, socketSize, lowLatency, false);
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
                final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize,
                final boolean lowLatency) {
            super(type, socketAddress, server, estimatedMaxMessageSize, lowLatency);
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
            NettySharedSocketEndpointFactory.this.onSocketChannel(socketChannel);
            super.onSocketChannel(socketChannel);
        }

    }

    private static final class NettySharedSocketEndpointFactoryFinalizer extends AWarningFinalizer {

        private volatile Bootstrap clientBootstrap;

        @Override
        protected void clean() {
            closeBootstrapAsync();
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
