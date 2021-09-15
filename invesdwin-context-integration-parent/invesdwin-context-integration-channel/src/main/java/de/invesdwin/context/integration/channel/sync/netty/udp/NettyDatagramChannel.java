package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.time.duration.Duration;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
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
    protected DatagramChannel datagramChannel;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    private Bootstrap bootstrap;

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

    public void open(final Consumer<Bootstrap> bootstrapListener, final Consumer<DatagramChannel> channelListener) {
        if (activeCount.incrementAndGet() > 1) {
            if (channelListener != null) {
                channelListener.accept(datagramChannel);
            }
            return;
        }
        if (server) {
            bootstrap = new Bootstrap();
            bootstrap.group(type.newServerWorkerGroup()).channel(type.getServerChannelType());
            //            type.channelOptions(bootstrap::option, socketSize);
            bootstrapListener.accept(bootstrap);
            try {
                datagramChannel = (DatagramChannel) bootstrap.bind(socketAddress).sync().channel();
                //                type.initChannel(datagramChannel, server);
                //                onDatagramChannel(datagramChannel);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            bootstrap = new Bootstrap();
            bootstrap.group(type.newClientWorkerGroup()).channel(type.getClientChannelType());
            //            type.channelOptions(bootstrap::option, socketSize);
            bootstrapListener.accept(bootstrap);
            try {
                //                type.initChannel(datagramChannel, server);
                datagramChannel = (DatagramChannel) bootstrap.connect(socketAddress).sync().channel();
                //                onDatagramChannel(datagramChannel);
            } catch (final Exception e) {
                throw new RuntimeException(e);
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
