package de.invesdwin.context.integration.channel.sync.netty.udt;

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
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.INettyUdtChannelType;
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
import io.netty.channel.udt.UdtChannel;

@ThreadSafe
public class NettySharedUdtClientEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {

    private final INettyUdtChannelType type;
    private final InetSocketAddress socketAddress;
    private final int socketSize;
    private final NettySharedUdtClientEndpointFactoryFinalizer bootstrapFinalizer;
    private final AtomicInteger bootstrapActiveCount = new AtomicInteger();
    @GuardedBy("self")
    private final IBufferingIterator<NettyUdtClientEndpointChannel> connectQueue = new BufferingIterator<>();

    public NettySharedUdtClientEndpointFactory(final INettyUdtChannelType type, final InetSocketAddress socketAddress,
            final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.socketSize = estimatedMaxMessageSize + NettyUdtSynchronousChannel.MESSAGE_INDEX;

        this.bootstrapFinalizer = new NettySharedUdtClientEndpointFactoryFinalizer();
        bootstrapFinalizer.register(this);
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final NettyUdtSynchronousChannel connector = new NettyUdtClientEndpointChannel(type, socketAddress, false,
                socketSize);
        final NettyUdtSynchronousReader reader = new NettyUdtSynchronousReader(connector);
        final NettyUdtSynchronousWriter writer = new NettyUdtSynchronousWriter(connector);
        return ImmutableSynchronousEndpoint.of(reader, writer);
    }

    private void maybeInitClientBootstrap() {
        if (bootstrapFinalizer.clientBootstrap == null) {
            synchronized (this) {
                if (bootstrapFinalizer.clientBootstrap == null) {
                    bootstrapFinalizer.clientBootstrap = new Bootstrap();
                    bootstrapFinalizer.clientBootstrap
                            .group(type.newClientWorkerGroup(newClientWorkerGroupThreadCount()));
                    bootstrapFinalizer.clientBootstrap.channelFactory(type.getClientChannelFactory());
                    type.channelOptions(bootstrapFinalizer.clientBootstrap::option, socketSize, false);
                    bootstrapFinalizer.clientBootstrap.handler(new ChannelInitializer<UdtChannel>() {
                        @Override
                        public void initChannel(final UdtChannel ch) throws Exception {
                            final NettyUdtClientEndpointChannel connector;
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
    protected void onUdtChannel(final UdtChannel udtChannel) {}

    protected SelectStrategyFactory newClientWorkerGroupSelectStrategyFactory() {
        return SelectStrategyFactories.DEFAULT;
    }

    protected int newClientWorkerGroupThreadCount() {
        return 1;
    }

    private final class NettyUdtClientEndpointChannel extends NettyUdtSynchronousChannel {

        @GuardedBy("this")
        private boolean opened;

        private NettyUdtClientEndpointChannel(final INettyUdtChannelType type, final InetSocketAddress socketAddress,
                final boolean server, final int estimatedMaxMessageSize) {
            super(type, socketAddress, server, estimatedMaxMessageSize);
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
        protected int newServerWorkerGroupThreadCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void open(final Consumer<UdtChannel> channelListener) throws IOException {
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
            awaitUdtChannel(() -> {
                return bootstrapFinalizer.clientBootstrap.connect(socketAddress);
            });
        }

        void onConnected(final UdtChannel ch) {
            type.initChannel(ch, false);
            onUdtChannel(ch);
            finalizer.udtChannel = ch;
        }

        @Override
        protected void onUdtChannel(final UdtChannel udtChannel) {
            NettySharedUdtClientEndpointFactory.this.onUdtChannel(udtChannel);
            super.onUdtChannel(udtChannel);
        }

    }

    private static final class NettySharedUdtClientEndpointFactoryFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private volatile Bootstrap clientBootstrap;

        protected NettySharedUdtClientEndpointFactoryFinalizer() {
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
            String warning = "Finalizing unclosed " + NettyUdtSynchronousChannel.class.getSimpleName();
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
