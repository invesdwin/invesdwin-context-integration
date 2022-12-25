package de.invesdwin.context.integration.channel.sync.netty.udt;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.INettyUdtChannelType;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.time.duration.Duration;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.udt.UdtChannel;

/**
 * https://github.com/wenzhucjy/netty-tutorial/tree/master/netty-4/server/src/main/java/com/netty/udp
 * 
 * https://github.com/wenzhucjy/netty-tutorial/tree/master/netty-4/client/src/main/java/com/netty/udp
 *
 */
@NotThreadSafe
public class NettyUdtSynchronousChannel implements Closeable {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final INettyUdtChannelType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile boolean udtChannelOpening;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    protected final NettyDatagramSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    private volatile boolean keepBootstrapRunningAfterOpen;

    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();

    public NettyUdtSynchronousChannel(final INettyUdtChannelType type, final InetSocketAddress socketAddress,
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

    public INettyUdtChannelType getType() {
        return type;
    }

    public UdtChannel getUdtChannel() {
        return finalizer.udtChannel;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public void open(final Consumer<Bootstrap> bootstrapListener, final Consumer<UdtChannel> channelListener)
            throws IOException {
        if (!shouldOpen(channelListener)) {
            return;
        }
        if (server) {
            awaitUdtChannel(() -> {
                finalizer.bootstrap = new Bootstrap();
                finalizer.bootstrap.group(type.newServerWorkerGroup()).channel(type.getServerChannelType());
                type.channelOptions(finalizer.bootstrap::option, socketSize);
                bootstrapListener.accept(finalizer.bootstrap);
                try {
                    return finalizer.bootstrap.bind(socketAddress);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            awaitUdtChannel(() -> {
                finalizer.bootstrap = new Bootstrap();
                finalizer.bootstrap.group(type.newClientWorkerGroup()).channel(type.getClientChannelType());
                type.channelOptions(finalizer.bootstrap::option, socketSize);
                bootstrapListener.accept(finalizer.bootstrap);
                try {
                    return finalizer.bootstrap.connect(socketAddress);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private synchronized boolean shouldOpen(final Consumer<UdtChannel> channelListener) {
        if (activeCount.incrementAndGet() > 1) {
            if (channelListener != null) {
                channelListener.accept(finalizer.udtChannel);
            }
            return false;
        } else {
            return true;
        }
    }

    private void awaitUdtChannel(final Supplier<ChannelFuture> channelFactory) throws IOException {
        udtChannelOpening = true;
        try {
            //init bootstrap
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while (activeCount.get() > 0) {
                try {
                    final ChannelFuture sync = channelFactory.get().sync();
                    type.initChannel(finalizer.udtChannel, server);
                    onUdtChannel(finalizer.udtChannel);
                    finalizer.udtChannel = (UdtChannel) sync.channel();
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
            udtChannelOpening = false;
        }
        awaitUdtChannel();
    }

    private void awaitUdtChannel() throws IOException {
        try {
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while ((finalizer.udtChannel == null || udtChannelOpening) && activeCount.get() > 0) {
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
    protected void onUdtChannel(final UdtChannel udtChannel) {
        //        final ChannelPipeline pipeline = udtChannel.pipeline();
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
    }

    private void internalClose() {
        finalizer.closeUdtChannel();
        final Bootstrap bootstrapCopy = finalizer.bootstrap;
        if (bootstrapCopy != null) {
            finalizer.bootstrap = null;
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
        finalizer.close();
    }

    public void closeBootstrapAsync() {
        finalizer.closeBootstrapAsync();
    }

    private static Future<?> shutdownGracefully(final EventLoopGroup group) {
        return NettySocketSynchronousChannel.shutdownGracefully(group);
    }

    private static void awaitShutdown(final Future<?> future) {
        NettySocketSynchronousChannel.awaitShutdown(future);
    }

    private static final class NettyDatagramSynchronousChannelFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private volatile UdtChannel udtChannel;
        private volatile Bootstrap bootstrap;

        protected NettyDatagramSynchronousChannelFinalizer() {
            if (Throwables.isDebugStackTraceEnabled()) {
                initStackTrace = new Exception();
                initStackTrace.fillInStackTrace();
            } else {
                initStackTrace = null;
            }
        }

        @Override
        protected void clean() {
            closeUdtChannel();
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
            return udtChannel == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

        private void closeUdtChannel() {
            final UdtChannel udtChannelCopy = udtChannel;
            if (udtChannelCopy != null) {
                udtChannel = null;
                udtChannelCopy.close();
            }
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

    }

}
