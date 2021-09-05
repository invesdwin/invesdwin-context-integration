package de.invesdwin.context.integration.channel.netty.udp;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.context.integration.channel.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.time.duration.Duration;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;

/**
 * https://github.com/wenzhucjy/netty-tutorial/tree/master/netty-4/server/src/main/java/com/netty/udp
 * 
 * https://github.com/wenzhucjy/netty-tutorial/tree/master/netty-4/client/src/main/java/com/netty/udp
 *
 */
@NotThreadSafe
public abstract class ANettyDatagramSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final INettyDatagramChannelType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected DatagramChannel datagramChannel;
    protected final SocketAddress socketAddress;
    protected final boolean server;
    private Bootstrap bootstrap;

    public ANettyDatagramSynchronousChannel(final INettyDatagramChannelType type, final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        if (server) {
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
                    } else {
                        //only allow one client
                        ch.close();
                    }
                }
            });
            final ChannelFuture channel = bootstrap.bind(socketAddress);
            awaitSocketChannel(channel);
        } else {
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
                    } else {
                        //only allow one client
                        ch.close();
                    }
                }
            });
            final ChannelFuture channel = bootstrap.connect(socketAddress);
            awaitSocketChannel(channel);
        }
    }

    private void awaitSocketChannel(final ChannelFuture channel) throws IOException {
        try {
            channel.sync();
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
            throw Throwables.propagate(t);
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
        if (datagramChannel != null) {
            datagramChannel.close();
            datagramChannel = null;
        }
        if (bootstrap != null) {
            final BootstrapConfig config = bootstrap.config();
            bootstrap = null;
            final EventLoopGroup group = config.group();
            if (group != null) {
                Futures.getNoInterrupt(group.shutdownGracefully());
            }
        }
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
