package de.invesdwin.context.integration.channel.sync.netty.udp;

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
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.socket.DatagramChannel;

@ThreadSafe
public class NettySharedDatagramEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {

    private final INettyDatagramChannelType type;
    private final InetSocketAddress socketAddress;
    private final int socketSize;
    private final boolean lowLatency;
    private final NettySharedDatagramEndpointFactoryFinalizer bootstrapFinalizer;
    private final AtomicInteger bootstrapActiveCount = new AtomicInteger();
    @GuardedBy("self")
    private final IBufferingIterator<NettyDatagramClientEndpointChannel> connectQueue = new BufferingIterator<>();

    public NettySharedDatagramEndpointFactory(final INettyDatagramChannelType type,
            final InetSocketAddress socketAddress, final int estimatedMaxMessageSize, final boolean lowLatency) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.socketSize = newSocketSize(estimatedMaxMessageSize);
        this.lowLatency = lowLatency;

        this.bootstrapFinalizer = new NettySharedDatagramEndpointFactoryFinalizer();
        bootstrapFinalizer.register(this);
    }

    protected int newSocketSize(final int estimatedMaxMessageSize) {
        return estimatedMaxMessageSize + NettyDatagramSynchronousChannel.MESSAGE_INDEX;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final NettyDatagramSynchronousChannel connector = new NettyDatagramClientEndpointChannel(type, socketAddress,
                false, socketSize, lowLatency);
        final NettyDatagramSynchronousReader reader = new NettyDatagramSynchronousReader(connector);
        final NettyDatagramSynchronousWriter writer = new NettyDatagramSynchronousWriter(connector);
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
                    bootstrapFinalizer.clientBootstrap.handler(new ChannelInitializer<DatagramChannel>() {
                        @Override
                        public void initChannel(final DatagramChannel ch) throws Exception {
                            final NettyDatagramClientEndpointChannel connector;
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
    protected void onDatagramChannel(final DatagramChannel datagramChannel) {}

    protected SelectStrategyFactory newClientWorkerGroupSelectStrategyFactory() {
        return SelectStrategyFactories.DEFAULT;
    }

    protected int newClientWorkerGroupThreadCount() {
        return 1;
    }

    public boolean isLowLatency() {
        return lowLatency;
    }

    private final class NettyDatagramClientEndpointChannel extends NettyDatagramSynchronousChannel {

        @GuardedBy("this")
        private boolean opened;

        private NettyDatagramClientEndpointChannel(final INettyDatagramChannelType type,
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
        protected SelectStrategyFactory newServerWorkerGroupSelectStrategyFactory() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int newServerWorkerGroupThreadCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void open(final Consumer<Bootstrap> bootstrapListener, final Consumer<DatagramChannel> channelListener)
                throws IOException {
            super.open(bootstrapListener, channelListener);
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
        protected void connect(final Consumer<Bootstrap> bootstrapListener) throws IOException {
            synchronized (connectQueue) {
                connectQueue.add(this);
            }
            maybeInitClientBootstrap();
            bootstrapListener.accept(bootstrapFinalizer.clientBootstrap);
            awaitDatagramChannel(() -> {
                return bootstrapFinalizer.clientBootstrap.connect(socketAddress);
            });
        }

        void onConnected(final DatagramChannel ch) {
            type.initChannel(ch, false);
            onDatagramChannel(ch);
            finalizer.datagramChannel = ch;
        }

        @Override
        protected void onDatagramChannel(final DatagramChannel datagramChannel) {
            NettySharedDatagramEndpointFactory.this.onDatagramChannel(datagramChannel);
            super.onDatagramChannel(datagramChannel);
        }

    }

    private static final class NettySharedDatagramEndpointFactoryFinalizer extends AWarningFinalizer {

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
