package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.time.duration.Duration;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.handler.flush.FlushConsolidationHandler;

/**
 * https://github.com/wenzhucjy/netty-tutorial/tree/master/netty-4/server/src/main/java/com/netty/udp
 * 
 * https://github.com/wenzhucjy/netty-tutorial/tree/master/netty-4/client/src/main/java/com/netty/udp
 *
 */
@NotThreadSafe
public class NettyDatagramChannel implements Closeable {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final INettyDatagramChannelType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected DatagramChannel datagramChannel;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    private Bootstrap bootstrap;

    private final IBufferingIterator<Consumer<DatagramChannel>> channelListeners = new BufferingIterator<>();
    private final AtomicInteger activeCount = new AtomicInteger();

    public NettyDatagramChannel(final INettyDatagramChannelType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
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

    public void open(final Consumer<DatagramChannel> channelListener) {
        if (activeCount.incrementAndGet() > 1) {
            if (channelListener != null) {
                channelListener.accept(datagramChannel);
            }
            return;
        }
        if (channelListener != null) {
            channelListeners.add(channelListener);
        }
        if (server) {
            awaitSocketChannel(() -> {
                final EventLoopGroup group = type.newServerWorkerGroup();
                bootstrap = new Bootstrap();
                bootstrap.group(group);
                bootstrap.channel(type.getServerChannelType());
                type.channelOptions(bootstrap::option, socketSize);
                bootstrap.handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    public void initChannel(final DatagramChannel ch) throws Exception {
                        if (datagramChannel == null) {
                            type.initChannel(ch, true);
                            datagramChannel = ch;
                            onDatagramChannel(ch);
                        } else {
                            //only allow one client
                            ch.close();
                        }
                    }
                });
                return bootstrap.bind(socketAddress);
            });
        } else {
            awaitSocketChannel(() -> {
                bootstrap = new Bootstrap();
                bootstrap.group(type.newClientWorkerGroup());
                bootstrap.channel(type.getClientChannelType());
                type.channelOptions(bootstrap::option, socketSize);
                bootstrap.handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    public void initChannel(final DatagramChannel ch) throws Exception {
                        if (datagramChannel == null) {
                            type.initChannel(ch, false);
                            datagramChannel = ch;
                            onDatagramChannel(ch);
                        } else {
                            //only allow one client
                            ch.close();
                        }
                    }
                });
                return bootstrap.connect(socketAddress);
            });
        }
    }

    /**
     * Can be overridden to add handlers
     */
    protected void onDatagramChannel(final DatagramChannel datagramChannel) {
        final ChannelPipeline pipeline = datagramChannel.pipeline();
        pipeline.addLast(new FlushConsolidationHandler(256, true));
        //        pipeline.addLast(new IdleStateHandler(1, 1, 1, TimeUnit.MILLISECONDS));
        triggerChannelListeners(datagramChannel);
    }

    private void triggerChannelListeners(final DatagramChannel channel) {
        try {
            while (true) {
                final Consumer<DatagramChannel> next = channelListeners.next();
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
            for (int tries = 0; datagramChannel == null; tries++) {
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
        if (datagramChannel != null) {
            datagramChannel.close();
            datagramChannel = null;
        }
        if (bootstrap != null) {
            final BootstrapConfig config = bootstrap.config();
            bootstrap = null;
            final EventLoopGroup group = config.group();
            awaitShutdown(shutdownGracefully(group));
        }
    }

    public void closeAsync() {
        if (activeCount.decrementAndGet() > 0) {
            return;
        }
        if (datagramChannel != null) {
            datagramChannel.close();
            datagramChannel = null;
        }
        if (bootstrap != null) {
            final BootstrapConfig config = bootstrap.config();
            bootstrap = null;
            final EventLoopGroup group = config.group();
            shutdownGracefully(group);
        }
    }

    private static Future<?> shutdownGracefully(final EventLoopGroup group) {
        return NettySocketChannel.shutdownGracefully(group);
    }

    private static void awaitShutdown(final Future<?> future) {
        NettySocketChannel.awaitShutdown(future);
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}