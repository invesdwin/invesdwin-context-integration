package de.invesdwin.context.integration.channel.netty;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.context.integration.channel.netty.type.INettyChannelType;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.time.duration.Duration;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.bootstrap.ServerBootstrapConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

@NotThreadSafe
public abstract class ANettySynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final INettyChannelType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected SocketChannel socketChannel;
    protected final SocketAddress socketAddress;
    protected final boolean server;
    private ServerBootstrap serverBootstrap;
    private Bootstrap clientBootstrap;

    public ANettySynchronousChannel(final INettyChannelType type, final SocketAddress socketAddress,
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
            final EventLoopGroup parentGroup = type.newServerBossGroup();
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
            final ChannelFuture channel = serverBootstrap.bind(socketAddress);
            awaitSocketChannel(channel);
        } else {
            clientBootstrap = new Bootstrap();
            clientBootstrap.group(type.newClientWorkerGroup());
            clientBootstrap.channel(type.getClientChannelType());
            type.channelOptions(serverBootstrap::option, socketSize);
            clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(final SocketChannel ch) throws Exception {
                    if (socketChannel == null) {
                        type.initChannel(ch, false);
                        socketChannel = ch;
                    } else {
                        //only allow one client
                        ch.close();
                    }
                }
            });
            final ChannelFuture channel = clientBootstrap.connect(socketAddress);
            awaitSocketChannel(channel);
        }
    }

    private void awaitSocketChannel(final ChannelFuture channel) throws IOException {
        try {
            channel.sync();
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
        if (socketChannel != null) {
            socketChannel.close();
            socketChannel = null;
        }
        if (serverBootstrap != null) {
            final ServerBootstrapConfig config = serverBootstrap.config();
            serverBootstrap = null;
            final EventLoopGroup childGroup = config.childGroup();
            final Future<?> childGroupShutdown;
            if (childGroup != null) {
                childGroupShutdown = childGroup.shutdownGracefully();
            } else {
                childGroupShutdown = null;
            }
            final EventLoopGroup group = config.group();
            final Future<?> groupShutdown;
            if (group != null && group != childGroup) {
                groupShutdown = group.shutdownGracefully();
            } else {
                groupShutdown = null;
            }
            if (childGroupShutdown != null) {
                Futures.getNoInterrupt(childGroupShutdown);
            }
            if (groupShutdown != null) {
                Futures.getNoInterrupt(groupShutdown);
            }
        }
        if (clientBootstrap != null) {
            final BootstrapConfig config = clientBootstrap.config();
            clientBootstrap = null;
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
