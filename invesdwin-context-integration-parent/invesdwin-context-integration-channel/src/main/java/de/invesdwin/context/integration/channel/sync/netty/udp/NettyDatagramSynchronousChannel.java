package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.netty.SelectStrategyFactories;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.time.duration.Duration;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.socket.DatagramChannel;

/**
 * https://github.com/wenzhucjy/netty-tutorial/tree/master/netty-4/server/src/main/java/com/netty/udp
 * 
 * https://github.com/wenzhucjy/netty-tutorial/tree/master/netty-4/client/src/main/java/com/netty/udp
 *
 */
@NotThreadSafe
public class NettyDatagramSynchronousChannel implements ISessionlessSynchronousChannel<InetSocketAddress> {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final INettyDatagramChannelType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile boolean datagramChannelOpening;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    protected final NettyDatagramSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    private volatile boolean keepBootstrapRunningAfterOpen;
    private volatile boolean multipleClientsAllowed;

    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();
    private InetSocketAddress otherSocketAddress;

    public NettyDatagramSynchronousChannel(final INettyDatagramChannelType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
        this.finalizer = new NettyDatagramSynchronousChannelFinalizer();
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

    public void setKeepBootstrapRunningAfterOpen() {
        this.keepBootstrapRunningAfterOpen = true;
    }

    public boolean isKeepBootstrapRunningAfterOpen() {
        return keepBootstrapRunningAfterOpen;
    }

    public void setMultipleClientsAllowed() {
        Assertions.checkTrue(isServer(), "only relevant for server channel");
        setKeepBootstrapRunningAfterOpen();
        this.multipleClientsAllowed = true;
    }

    public boolean isMultipleClientsAllowed() {
        return multipleClientsAllowed;
    }

    public INettyDatagramChannelType getType() {
        return type;
    }

    public DatagramChannel getDatagramChannel() {
        return finalizer.datagramChannel;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    @Override
    public void setOtherSocketAddress(final InetSocketAddress otherSocketAddress) {
        this.otherSocketAddress = otherSocketAddress;
    }

    @Override
    public InetSocketAddress getOtherSocketAddress() {
        return otherSocketAddress;
    }

    @Deprecated
    @Override
    public void open() throws IOException {
        //noop
    }

    public void open(final Consumer<Bootstrap> bootstrapListener, final Consumer<DatagramChannel> channelListener)
            throws IOException {
        if (!shouldOpen(channelListener)) {
            return;
        }
        if (server) {
            awaitDatagramChannel(() -> {
                finalizer.bootstrap = new Bootstrap();
                finalizer.bootstrap.group(type.newServerWorkerGroup(newServerWorkerGroupThreadCount(),
                        newServerWorkerGroupSelectStrategyFactory())).channel(type.getServerChannelType());
                type.channelOptions(finalizer.bootstrap::option, socketSize, server);
                bootstrapListener.accept(finalizer.bootstrap);
                try {
                    return finalizer.bootstrap.bind(socketAddress);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            connect(bootstrapListener);
        }
    }

    protected void connect(final Consumer<Bootstrap> bootstrapListener) throws IOException {
        awaitDatagramChannel(() -> {
            finalizer.bootstrap = new Bootstrap();
            finalizer.bootstrap.group(type.newClientWorkerGroup(newClientWorkerGroupThreadCount(),
                    newClientWorkerGroupSelectStrategyFactory())).channel(type.getClientChannelType());
            type.channelOptions(finalizer.bootstrap::option, socketSize, server);
            bootstrapListener.accept(finalizer.bootstrap);
            try {
                return finalizer.bootstrap.connect(socketAddress);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected SelectStrategyFactory newClientWorkerGroupSelectStrategyFactory() {
        return newServerWorkerGroupSelectStrategyFactory();
    }

    protected SelectStrategyFactory newServerWorkerGroupSelectStrategyFactory() {
        if (multipleClientsAllowed) {
            return SelectStrategyFactories.DEFAULT;
        } else {
            return SelectStrategyFactories.BUSY_WAIT;
        }
    }

    protected int newClientWorkerGroupThreadCount() {
        return 1;
    }

    protected int newServerWorkerGroupThreadCount() {
        return 1;
    }

    private synchronized boolean shouldOpen(final Consumer<DatagramChannel> channelListener) throws IOException {
        if (activeCount.incrementAndGet() > 1) {
            if (multipleClientsAllowed) {
                throw new IllegalStateException(
                        "multiple opens when multiple clients are allowed are not supported, use an asynchronous handler for that purpose");
            }
            awaitDatagramChannel();
            if (channelListener != null) {
                channelListener.accept(finalizer.datagramChannel);
            }
            return false;
        } else {
            return true;
        }
    }

    protected void awaitDatagramChannel(final Supplier<ChannelFuture> channelFactory) throws IOException {
        datagramChannelOpening = true;
        try {
            //init bootstrap
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while (activeCount.get() > 0) {
                try {
                    final ChannelFuture sync = channelFactory.get().sync();
                    type.initChannel(finalizer.datagramChannel, server);
                    onDatagramChannel(finalizer.datagramChannel);
                    if (!multipleClientsAllowed) {
                        finalizer.datagramChannel = (DatagramChannel) sync.channel();
                    }
                    sync.get();
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
            datagramChannelOpening = false;
        }
        awaitDatagramChannel();
    }

    private void awaitDatagramChannel() throws IOException {
        try {
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while (!multipleClientsAllowed && (finalizer.datagramChannel == null || datagramChannelOpening)
                    && activeCount.get() > 0) {
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

    /**
     * Can be overridden to add handlers
     */
    protected void onDatagramChannel(final DatagramChannel datagramChannel) {
        //        final ChannelPipeline pipeline = datagramChannel.pipeline();
        //        pipeline.addLast(new FlushConsolidationHandler(256, true));
        //        pipeline.addLast(new IdleStateHandler(1, 1, 1, TimeUnit.MILLISECONDS));
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
        otherSocketAddress = null;
    }

    public boolean isClosed() {
        return finalizer.isCleaned();
    }

    private void internalClose() {
        finalizer.closeDatagramChannel();
        final Bootstrap bootstrapCopy = finalizer.bootstrap;
        if (bootstrapCopy != null) {
            finalizer.bootstrap = null;
            final BootstrapConfig config = bootstrapCopy.config();
            final EventLoopGroup group = config.group();
            NettySocketSynchronousChannel.awaitShutdown(NettySocketSynchronousChannel.shutdownGracefully(group));
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

    protected static final class NettyDatagramSynchronousChannelFinalizer extends AWarningFinalizer {

        protected volatile DatagramChannel datagramChannel;
        private volatile Bootstrap bootstrap;

        @Override
        protected void clean() {
            closeDatagramChannel();
            closeBootstrapAsync();
        }

        @Override
        protected boolean isCleaned() {
            return datagramChannel == null && bootstrap == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

        private void closeDatagramChannel() {
            final DatagramChannel datagramChannelCopy = datagramChannel;
            if (datagramChannelCopy != null) {
                datagramChannel = null;
                datagramChannelCopy.close();
            }
        }

        public void closeBootstrapAsync() {
            final Bootstrap bootstrapCopy = bootstrap;
            if (bootstrapCopy != null) {
                bootstrap = null;
                final BootstrapConfig config = bootstrapCopy.config();
                final EventLoopGroup group = config.group();
                NettySocketSynchronousChannel.shutdownGracefully(group);
            }
        }

    }

}
