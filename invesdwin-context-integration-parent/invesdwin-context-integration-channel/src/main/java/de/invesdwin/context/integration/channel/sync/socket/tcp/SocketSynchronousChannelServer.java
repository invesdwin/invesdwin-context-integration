package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AFinalizer;

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

    private static final class SocketSynchronousChannelFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private volatile ServerSocketChannel serverSocketChannel;
        private SocketChannel pendingSocketChannel;

        protected SocketSynchronousChannelFinalizer() {
            if (Throwables.isDebugStackTraceEnabled()) {
                initStackTrace = new Exception();
                initStackTrace.fillInStackTrace();
            } else {
                initStackTrace = null;
            }
        }

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
        protected void onRun() {
            String warning = "Finalizing unclosed " + SocketSynchronousChannel.class.getSimpleName();
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
        finalizer.pendingSocketChannel = finalizer.serverSocketChannel.accept();
        return finalizer.pendingSocketChannel != null;
    }

    @Override
    public SocketSynchronousChannel readMessage() throws IOException {
        final SocketSynchronousChannel channel = newSocketSynchronousChannel(finalizer.pendingSocketChannel);
        finalizer.pendingSocketChannel = null;
        return channel;
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketChannel pendingSocketChannel)
            throws IOException {
        return new SocketSynchronousChannel(this, pendingSocketChannel);
    }

    @Override
    public void readFinished() throws IOException {
        //noop
    }

}
