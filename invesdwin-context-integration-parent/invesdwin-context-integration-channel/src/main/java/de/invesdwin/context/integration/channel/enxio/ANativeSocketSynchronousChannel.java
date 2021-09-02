package de.invesdwin.context.integration.channel.enxio;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.util.time.duration.Duration;
import jnr.enxio.channels.NativeServerSocketChannel;
import jnr.enxio.channels.NativeSocketChannel;

@NotThreadSafe
public abstract class ANativeSocketSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected NativeSocketChannel socketChannel;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    protected NativeServerSocketChannel serverSocketChannel;

    public ANativeSocketSynchronousChannel(final InetSocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        if (server) {
            //            serverSocketChannel = new NativeServerSocketChannel(fd, ops);
            //            serverSocketChannel.bind(socketAddress);
            //            socketChannel = type.acceptSocketChannel(serverSocketChannel);
        } else {
            for (int tries = 0;; tries++) {
                //                socketChannel = new NativeSocketChannel(fs, ops);
                //                try {
                //                    socketChannel.connect(socketAddress);
                //                    break;
                //                } catch (final ConnectException e) {
                //                    socketChannel.close();
                //                    socketChannel = null;
                //                    if (tries < getMaxConnectRetries()) {
                //                        try {
                //                            getConnectRetryDelay().sleep();
                //                        } catch (final InterruptedException e1) {
                //                            throw new RuntimeException(e1);
                //                        }
                //                    } else {
                //                        throw e;
                //                    }
                //                }
            }
        }
        //non-blocking sockets are a bit faster than blocking ones
        socketChannel.configureBlocking(false);
    }

    protected Duration getConnectRetryDelay() {
        return Duration.ONE_SECOND;
    }

    protected int getMaxConnectRetries() {
        return 10;
    }

    @Override
    public void close() throws IOException {
        if (socketChannel != null) {
            socketChannel.close();
            socketChannel = null;
        }
        if (serverSocketChannel != null) {
            serverSocketChannel.close();
            serverSocketChannel = null;
        }
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
