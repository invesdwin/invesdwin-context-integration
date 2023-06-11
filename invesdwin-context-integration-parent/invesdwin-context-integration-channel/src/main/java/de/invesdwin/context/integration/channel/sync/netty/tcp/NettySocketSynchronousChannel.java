package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.time.duration.Duration;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.bootstrap.ServerBootstrapConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

@ThreadSafe
public class NettySocketSynchronousChannel implements Closeable {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final INettySocketChannelType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    private volatile boolean socketChannelOpening;
    private final NettySocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    private volatile boolean keepBootstrapRunningAfterOpen;
    private volatile boolean multipleClientsAllowed;

    private final List<Consumer<SocketChannel>> channelListeners = new ArrayList<>();
    private final AtomicInteger activeCount = new AtomicInteger();

    public NettySocketSynchronousChannel(final INettySocketChannelType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        this.type = type;
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

    public INettySocketChannelType getType() {
        return type;
    }

    public SocketChannel getSocketChannel() {
        return finalizer.socketChannel;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public synchronized void addChannelListener(final Consumer<SocketChannel> channelListener) {
        if (channelListener != null) {
            channelListeners.add(channelListener);
        }
    }

    public void open(final Consumer<SocketChannel> channelListener) throws IOException {
        if (!shouldOpen(channelListener)) {
            return;
        }
        addChannelListener(channelListener);
        if (server) {
            awaitSocketChannel(() -> {
                final EventLoopGroup parentGroup = type.newServerAcceptorGroup(newServerAcceptorGroupThreadCount());
                final EventLoopGroup childGroup = type.newServerWorkerGroup(newServerWorkerGroupThreadCount(),
                        parentGroup);
                finalizer.serverBootstrap = new ServerBootstrap();
                finalizer.serverBootstrap.group(parentGroup, childGroup);
                finalizer.serverBootstrap.channel(type.getServerChannelType());
                type.channelOptions(finalizer.serverBootstrap::childOption, socketSize, server);
                finalizer.serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(final SocketChannel ch) throws Exception {
                        if (multipleClientsAllowed) {
                            type.initChannel(ch, true);
                            onSocketChannel(ch);
                        } else {
                            if (finalizer.socketChannel == null) {
                                type.initChannel(ch, true);
                                onSocketChannel(ch);
                                finalizer.socketChannel = ch;
                                if (parentGroup != childGroup) {
                                    //only allow one client
                                    parentGroup.shutdownGracefully();
                                }
                            } else {
                                //only allow one client
                                ch.close();
                            }
                        }
                    }
                });
                return finalizer.serverBootstrap.bind(socketAddress);
            });
        } else {
            awaitSocketChannel(() -> {
                finalizer.clientBootstrap = new Bootstrap();
                finalizer.clientBootstrap.group(type.newClientWorkerGroup(newClientWorkerGroupThreadCount()));
                finalizer.clientBootstrap.channel(type.getClientChannelType());
                type.channelOptions(finalizer.clientBootstrap::option, socketSize, server);
                finalizer.clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(final SocketChannel ch) throws Exception {
                        if (finalizer.socketChannel == null) {
                            type.initChannel(ch, false);
                            onSocketChannel(ch);
                            finalizer.socketChannel = ch;
                        } else {
                            //only allow one client
                            ch.close();
                        }
                    }

                });
                return finalizer.clientBootstrap.connect(socketAddress);
            });
        }
    }

    protected int newClientWorkerGroupThreadCount() {
        return 1;
    }

    protected int newServerWorkerGroupThreadCount() {
        return 1;
    }

    protected int newServerAcceptorGroupThreadCount() {
        return 1;
    }

    private synchronized boolean shouldOpen(final Consumer<SocketChannel> channelListener) throws IOException {
        if (activeCount.incrementAndGet() > 1) {
            if (multipleClientsAllowed) {
                throw new IllegalStateException(
                        "multiple opens when multiple clients are allowed are not supported, use an asynchronous handler for that purpose");
            }
            awaitSocketChannel();
            if (channelListener != null) {
                channelListener.accept(finalizer.socketChannel);
            }
            return false;
        } else {
            return true;
        }
    }

    /**
     * Can be overridden to add handlers
     */
    protected void onSocketChannel(final SocketChannel socketChannel) {
        //        final ChannelPipeline pipeline = socketChannel.pipeline();
        //        pipeline.addLast(new FlushConsolidationHandler(256, true));
        //        pipeline.addLast(new IdleStateHandler(1, 1, 1, TimeUnit.MILLISECONDS));
        triggerChannelListeners(socketChannel);
    }

    private void triggerChannelListeners(final SocketChannel channel) {
        for (int i = 0; i < channelListeners.size(); i++) {
            final Consumer<SocketChannel> channelListener = channelListeners.get(i);
            channelListener.accept(channel);
        }
    }

    private void awaitSocketChannel(final Supplier<ChannelFuture> channelFactory) throws IOException {
        socketChannelOpening = true;
        try {
            //init bootstrap
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while (activeCount.get() > 0) {
                try {
                    channelFactory.get().sync().get();
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
            socketChannelOpening = false;
        }
        awaitSocketChannel();
    }

    private void awaitSocketChannel() throws IOException {
        try {
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            //wait for channel
            while (!multipleClientsAllowed && (finalizer.socketChannel == null || socketChannelOpening)
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

    public boolean isClosed() {
        return finalizer.isCleaned();
    }

    private void internalClose() {
        finalizer.closeSocketChannel();
        final ServerBootstrap serverBootstrapCopy = finalizer.serverBootstrap;
        if (serverBootstrapCopy != null) {
            finalizer.serverBootstrap = null;
            final ServerBootstrapConfig config = serverBootstrapCopy.config();
            final EventLoopGroup childGroup = config.childGroup();
            final Future<?> childGroupShutdown = shutdownGracefully(childGroup);
            final EventLoopGroup group = config.group();
            final Future<?> groupShutdown = shutdownGracefully(group);
            awaitShutdown(childGroupShutdown);
            awaitShutdown(groupShutdown);
        }
        final Bootstrap clientBootstrapCopy = finalizer.clientBootstrap;
        if (clientBootstrapCopy != null) {
            finalizer.clientBootstrap = null;
            final BootstrapConfig config = clientBootstrapCopy.config();
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

    public static Future<?> shutdownGracefully(final EventLoopGroup group) {
        if (group == null) {
            return null;
        }
        return group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
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
        private volatile SocketChannel socketChannel;
        private volatile ServerBootstrap serverBootstrap;
        private volatile Bootstrap clientBootstrap;

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
            closeSocketChannel();
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
            return socketChannel == null && serverBootstrap == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

        public void closeSocketChannel() {
            final SocketChannel socketChannelCopy = socketChannel;
            if (socketChannelCopy != null) {
                socketChannel = null;
                socketChannelCopy.close();
            }
        }

        public void closeBootstrapAsync() {
            if (serverBootstrap != null) {
                final ServerBootstrapConfig config = serverBootstrap.config();
                serverBootstrap = null;
                final EventLoopGroup childGroup = config.childGroup();
                shutdownGracefully(childGroup);
                final EventLoopGroup group = config.group();
                shutdownGracefully(group);
            }
            if (clientBootstrap != null) {
                final BootstrapConfig config = clientBootstrap.config();
                clientBootstrap = null;
                final EventLoopGroup group = config.group();
                shutdownGracefully(group);
            }
        }

    }

}
