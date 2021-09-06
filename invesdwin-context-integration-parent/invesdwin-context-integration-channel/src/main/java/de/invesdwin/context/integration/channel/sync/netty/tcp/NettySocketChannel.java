package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

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
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.timeout.IdleStateHandler;

@ThreadSafe
public class NettySocketChannel implements Closeable {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final INettySocketChannelType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected SocketChannel socketChannel;
    protected final SocketAddress socketAddress;
    protected final boolean server;
    private ServerBootstrap serverBootstrap;
    private Bootstrap clientBootstrap;

    private final IBufferingIterator<Consumer<SocketChannel>> channelListeners = new BufferingIterator<>();
    private final AtomicInteger activeCount = new AtomicInteger();

    public NettySocketChannel(final INettySocketChannelType type, final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    public void addSocketChannelListeners(final Consumer<SocketChannel> listener) {
        channelListeners.add(listener);
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public void open(final Consumer<SocketChannel> channelListener) {
        if (activeCount.incrementAndGet() > 1) {
            if (channelListener != null) {
                channelListener.accept(socketChannel);
            }
            return;
        }
        if (channelListener != null) {
            channelListeners.add(channelListener);
        }
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
                            socketChannel = ch;
                            onSocketChannel(ch);
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
                            socketChannel = ch;
                            onSocketChannel(ch);
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

    /**
     * Can be overridden to add handlers
     */
    protected void onSocketChannel(final SocketChannel socketChannel) {
        final ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast(new FlushConsolidationHandler(256, true));
        pipeline.addLast(new IdleStateHandler(1, 1, 1, TimeUnit.MILLISECONDS));
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
        try {
            //init bootstrap
            for (int tries = 0;; tries++) {
                try {
                    channelFactory.get().sync().get();
                    break;
                } catch (final Throwable t) {
                    close();
                    if (tries < getMaxConnectRetries()) {
                        try {
                            getConnectRetryDelay().sleep();
                        } catch (final InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                    } else {
                        throw t;
                    }
                }
            }
            //wait for channel
            for (int tries = 0; socketChannel == null; tries++) {
                if (tries < getMaxConnectRetries()) {
                    try {
                        getConnectRetryDelay().sleep();
                    } catch (final InterruptedException e1) {
                        throw new RuntimeException(e1);
                    }
                } else {
                    throw new ConnectException("Connection timeout");
                }
            }
        } catch (final Throwable t) {
            close();
            throw new RuntimeException(t);
        }
    }

    protected Duration getConnectRetryDelay() {
        return Duration.ONE_SECOND;
    }

    protected int getMaxConnectRetries() {
        return 10;
    }

    @Override
    public void close() {
        if (activeCount.decrementAndGet() > 0) {
            return;
        }
        if (socketChannel != null) {
            socketChannel.close();
            socketChannel = null;
        }
        if (serverBootstrap != null) {
            final ServerBootstrapConfig config = serverBootstrap.config();
            serverBootstrap = null;
            final EventLoopGroup childGroup = config.childGroup();
            final Future<?> childGroupShutdown = shutdownGracefully(childGroup);
            final EventLoopGroup group = config.group();
            final Future<?> groupShutdown = shutdownGracefully(group);
            awaitShutdown(childGroupShutdown);
            awaitShutdown(groupShutdown);
        }
        if (clientBootstrap != null) {
            final BootstrapConfig config = clientBootstrap.config();
            clientBootstrap = null;
            final EventLoopGroup group = config.group();
            awaitShutdown(shutdownGracefully(group));
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

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
