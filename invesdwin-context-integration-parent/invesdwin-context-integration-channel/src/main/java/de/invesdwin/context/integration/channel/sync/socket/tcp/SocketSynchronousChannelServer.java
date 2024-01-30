package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;

@NotThreadSafe
public class SocketSynchronousChannelServer implements ISynchronousReader<SocketSynchronousChannel> {

    protected final int estimatedMaxMessageSize;
    protected final SocketAddress socketAddress;
    private final SocketSynchronousChannelFinalizer finalizer;

    public SocketSynchronousChannelServer(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.finalizer = new SocketSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public int getEstimatedMaxMessageSize() {
        return estimatedMaxMessageSize;
    }

    public ServerSocketChannel getServerSocketChannel() {
        return finalizer.serverSocketChannel;
    }

    @Override
    public void open() throws IOException {
        if (finalizer.serverSocketChannel != null) {
            throw new IllegalStateException("Already opened");
        }
        finalizer.serverSocketChannel = newServerSocketChannel();
        finalizer.serverSocketChannel.bind(socketAddress);
        finalizer.serverSocketChannel.configureBlocking(false);
    }

    protected ServerSocketChannel newServerSocketChannel() throws IOException {
        return ServerSocketChannel.open();
    }

    @Override
    public void close() {
        finalizer.close();
    }

    private static final class SocketSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile ServerSocketChannel serverSocketChannel;
        private SocketChannel pendingSocketChannel;

        @Override
        protected void clean() {
            final SocketChannel socketChannelCopy = pendingSocketChannel;
            if (socketChannelCopy != null) {
                pendingSocketChannel = null;
                Closeables.closeQuietly(socketChannelCopy);
            }
            final ServerSocketChannel serverSocketChannelCopy = serverSocketChannel;
            if (serverSocketChannelCopy != null) {
                serverSocketChannel = null;
                Closeables.closeQuietly(serverSocketChannelCopy);
            }
        }

        @Override
        protected boolean isCleaned() {
            return serverSocketChannel == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

    @Override
    public boolean hasNext() throws IOException {
        if (finalizer.pendingSocketChannel != null) {
            return true;
        }
        final ServerSocketChannel serverSocketChannelCopy = finalizer.serverSocketChannel;
        if (serverSocketChannelCopy == null) {
            throw FastEOFException.getInstance("closed");
        }
        finalizer.pendingSocketChannel = serverSocketChannelCopy.accept();
        return finalizer.pendingSocketChannel != null;
    }

    @Override
    public SocketSynchronousChannel readMessage() throws IOException {
        final SocketChannel socketChannel = finalizer.pendingSocketChannel;
        finalizer.pendingSocketChannel = null;
        final SocketSynchronousChannel channel = newSocketSynchronousChannel(socketChannel);
        return channel;
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketChannel socketChannel)
            throws IOException {
        return new SocketSynchronousChannel(this, socketChannel);
    }

    @Override
    public void readFinished() throws IOException {
        //noop
    }

}
