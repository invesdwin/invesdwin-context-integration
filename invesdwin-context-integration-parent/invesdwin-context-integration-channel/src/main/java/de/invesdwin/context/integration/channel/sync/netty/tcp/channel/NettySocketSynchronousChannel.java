package de.invesdwin.context.integration.channel.sync.netty.tcp.channel;

import java.io.Closeable;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.future.Futures;
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
    protected volatile SocketChannel socketChannel;
    protected volatile boolean socketChannelOpening;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    private volatile ServerBootstrap serverBootstrap;
    private volatile Bootstrap clientBootstrap;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    private volatile boolean keepBootstrapRunningAfterOpen;

    private final IBufferingIterator<Consumer<SocketChannel>> channelListeners = new BufferingIterator<>();
    private final AtomicInteger activeCount = new AtomicInteger();

    public NettySocketSynchronousChannel(final INettySocketChannelType type, final InetSocketAddress socketAddress,
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

    public INettySocketChannelType getType() {
        return type;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
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

    public void open(final Consumer<SocketChannel> channelListener) {
        if (!shouldOpen(channelListener)) {
            awaitSocketChannel();
            return;
        }
        addChannelListener(channelListener);
        if (server) {
            awaitSocketChannel(() -> {
                final EventLoopGroup parentGroup = type.newServerAcceptorGroup();
                final EventLoopGroup childGroup = type.newServerWorkerGroup(parentGroup);
                serverBootstrap = new ServerBootstrap();
                serverBootstrap.group(parentGroup, childGroup);
                serverBootstrap.channel(type.getServerChannelType());
                type.channelOptions(serverBootstrap::childOption, socketSize);
                serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(final SocketChannel ch) throws Exception {
                        if (socketChannel == null) {
                            type.initChannel(ch, true);
                            onSocketChannel(ch);
                            socketChannel = ch;
                            if (parentGroup != childGroup) {
                                //only allow one client
                                parentGroup.shutdownGracefully();
                            }
                        } else {
                            //only allow one client
                            ch.close();
                        }
                    }
                });
                return serverBootstrap.bind(socketAddress);
            });
        } else {
            awaitSocketChannel(() -> {
                clientBootstrap = new Bootstrap();
                clientBootstrap.group(type.newClientWorkerGroup());
                clientBootstrap.channel(type.getClientChannelType());
                type.channelOptions(clientBootstrap::option, socketSize);
                clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(final SocketChannel ch) throws Exception {
                        if (socketChannel == null) {
                            type.initChannel(ch, false);
                            onSocketChannel(ch);
                            socketChannel = ch;
                        } else {
                            //only allow one client
                            ch.close();
                        }
                    }

                });
                return clientBootstrap.connect(socketAddress);
            });
        }
    }

    private synchronized boolean shouldOpen(final Consumer<SocketChannel> channelListener) {
        if (activeCount.incrementAndGet() > 1) {
            if (channelListener != null) {
                channelListener.accept(socketChannel);
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
        try {
            while (true) {
                final Consumer<SocketChannel> next = channelListeners.next();
                next.accept(channel);
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
    }

    private void awaitSocketChannel(final Supplier<ChannelFuture> channelFactory) {
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
            socketChannelOpening = false;
        }
        awaitSocketChannel();
    }

    private void awaitSocketChannel() {
        try {
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            //wait for channel
            while ((socketChannel == null || socketChannelOpening) && activeCount.get() > 0) {
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
            if (activeCount.get() > 0 && activeCount.decrementAndGet() > 0) {
                return;
            }
        }
        internalClose();
    }

    private void internalClose() {
        closeSocketChannel();
        final ServerBootstrap serverBootstrapCopy = serverBootstrap;
        if (serverBootstrapCopy != null) {
            serverBootstrap = null;
            final ServerBootstrapConfig config = serverBootstrapCopy.config();
            final EventLoopGroup childGroup = config.childGroup();
            final Future<?> childGroupShutdown = shutdownGracefully(childGroup);
            final EventLoopGroup group = config.group();
            final Future<?> groupShutdown = shutdownGracefully(group);
            awaitShutdown(childGroupShutdown);
            awaitShutdown(groupShutdown);
        }
        final Bootstrap clientBootstrapCopy = clientBootstrap;
        if (clientBootstrapCopy != null) {
            clientBootstrap = null;
            final BootstrapConfig config = clientBootstrapCopy.config();
            final EventLoopGroup group = config.group();
            awaitShutdown(shutdownGracefully(group));
        }
    }

    public void closeAsync() {
        synchronized (this) {
            if (activeCount.get() > 0 && activeCount.decrementAndGet() > 0) {
                return;
            }
        }
        closeSocketChannel();
        closeBootstrapAsync();
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

}
