package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.io.Closeable;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.NettySocketChannel;
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
public class NettyDatagramChannel implements Closeable {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final INettyDatagramChannelType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile DatagramChannel datagramChannel;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    private volatile Bootstrap bootstrap;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    private volatile boolean keepBootstrapRunningAfterOpen;

    private final AtomicInteger activeCount = new AtomicInteger();

    public NettyDatagramChannel(final INettyDatagramChannelType type, final InetSocketAddress socketAddress,
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
        synchronized (this) {
            if (activeCount.incrementAndGet() > 1) {
                if (channelListener != null) {
                    channelListener.accept(datagramChannel);
                }
                return;
            }
        }
        if (server) {
            awaitSocketChannel(() -> {
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
            awaitSocketChannel(() -> {
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

    private void awaitSocketChannel(final Supplier<ChannelFuture> channelFactory) {
        try {
            //init bootstrap
            for (int tries = 0; activeCount.get() > 0; tries++) {
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
                        if (tries < getMaxConnectRetries()) {
                            try {
                                getConnectRetryDelay().sleep();
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
            //wait for channel
            for (int tries = 0; datagramChannel == null && activeCount.get() > 0; tries++) {
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
            if (activeCount.get() > 0) {
                internalClose();
                throw new RuntimeException(t);
            }
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

    protected Duration getConnectRetryDelay() {
        return Duration.ONE_SECOND;
    }

    protected int getMaxConnectRetries() {
        return 10;
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
        if (datagramChannel != null) {
            datagramChannel.close();
            datagramChannel = null;
        }
        closeBootstrap();
    }

    private void closeBootstrap() {
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
        closeBootstrapAsync();
    }

    public void closeBootstrapAsync() {
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

}
