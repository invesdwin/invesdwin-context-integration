package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.io.Closeable;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.time.duration.Duration;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;

/**
 * https://github.com/wenzhucjy/netty-tutorial/tree/master/netty-4/server/src/main/java/com/netty/udp
 * 
 * https://github.com/wenzhucjy/netty-tutorial/tree/master/netty-4/client/src/main/java/com/netty/udp
 *
 */
@NotThreadSafe
public class NettyDatagramSynchronousChannel implements Closeable {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final INettyDatagramChannelType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile DatagramChannel datagramChannel;
    protected volatile boolean datagramChannelOpening;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    private volatile Bootstrap bootstrap;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    private volatile boolean keepBootstrapRunningAfterOpen;

    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();

    public NettyDatagramSynchronousChannel(final INettyDatagramChannelType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
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

    public INettyDatagramChannelType getType() {
        return type;
    }

    public DatagramChannel getDatagramChannel() {
        return datagramChannel;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public void open(final Consumer<Bootstrap> bootstrapListener, final Consumer<DatagramChannel> channelListener) {
        if (!shouldOpen(channelListener)) {
            return;
        }
        if (server) {
            awaitDatagramChannel(() -> {
                bootstrap = new Bootstrap();
                bootstrap.group(type.newServerWorkerGroup()).channel(type.getServerChannelType());
                type.channelOptions(bootstrap::option, socketSize);
                bootstrapListener.accept(bootstrap);
                try {
                    return bootstrap.bind(socketAddress);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            awaitDatagramChannel(() -> {
                bootstrap = new Bootstrap();
                bootstrap.group(type.newClientWorkerGroup()).channel(type.getClientChannelType());
                type.channelOptions(bootstrap::option, socketSize);
                bootstrapListener.accept(bootstrap);
                try {
                    return bootstrap.connect(socketAddress);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private synchronized boolean shouldOpen(final Consumer<DatagramChannel> channelListener) {
        if (activeCount.incrementAndGet() > 1) {
            if (channelListener != null) {
                channelListener.accept(datagramChannel);
            }
            return false;
        } else {
            return true;
        }
    }

    private void awaitDatagramChannel(final Supplier<ChannelFuture> channelFactory) {
        datagramChannelOpening = true;
        try {
            //init bootstrap
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while (activeCount.get() > 0) {
                try {
                    final ChannelFuture sync = channelFactory.get().sync();
                    type.initChannel(datagramChannel, server);
                    onDatagramChannel(datagramChannel);
                    datagramChannel = (DatagramChannel) sync.channel();
                    sync.get();
                    break;
                } catch (final Throwable t) {
                    if (activeCount.get() > 0) {
                        internalClose();
                        if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                            try {
                                getMaxConnectRetryDelay().sleepRandom();
                            } catch (final InterruptedException e1) {
                                throw new RuntimeException(e1);
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
            throw new RuntimeException(t);
        } finally {
            datagramChannelOpening = false;
        }
        awaitDatagramChannel();
    }

    private void awaitDatagramChannel() {
        try {
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while ((datagramChannel == null || datagramChannelOpening) && activeCount.get() > 0) {
                if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                    getWaitInterval().sleep();
                } else {
                    throw new ConnectException("Connection timeout");
                }
            }
        } catch (final Throwable t) {
            closeAsync();
            throw new RuntimeException(t);
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
    }

    private void internalClose() {
        final DatagramChannel datagramChannelCopy = datagramChannel;
        if (datagramChannelCopy != null) {
            datagramChannel = null;
            datagramChannelCopy.close();
        }
        closeBootstrap();
    }

    private void closeBootstrap() {
        final Bootstrap bootstrapCopy = bootstrap;
        if (bootstrapCopy != null) {
            bootstrap = null;
            final BootstrapConfig config = bootstrapCopy.config();
            final EventLoopGroup group = config.group();
            awaitShutdown(shutdownGracefully(group));
        }
    }

    public void closeAsync() {
        synchronized (this) {
            if (activeCount.get() > 0) {
                activeCount.decrementAndGet();
            }
        }
        final DatagramChannel datagramChannelCopy = datagramChannel;
        if (datagramChannelCopy != null) {
            datagramChannel = null;
            datagramChannelCopy.close();
        }
        closeBootstrapAsync();
    }

    public void closeBootstrapAsync() {
        final Bootstrap bootstrapCopy = bootstrap;
        if (bootstrapCopy != null) {
            bootstrap = null;
            final BootstrapConfig config = bootstrapCopy.config();
            final EventLoopGroup group = config.group();
            shutdownGracefully(group);
        }
    }

    private static Future<?> shutdownGracefully(final EventLoopGroup group) {
        return NettySocketSynchronousChannel.shutdownGracefully(group);
    }

    private static void awaitShutdown(final Future<?> future) {
        NettySocketSynchronousChannel.awaitShutdown(future);
    }

}
