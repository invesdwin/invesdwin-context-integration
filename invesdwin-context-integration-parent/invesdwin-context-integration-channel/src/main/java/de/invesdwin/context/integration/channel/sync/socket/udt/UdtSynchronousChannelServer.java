package de.invesdwin.context.integration.channel.sync.socket.udt;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import com.barchart.udt.nio.SelectorProviderUDT;
import com.barchart.udt.nio.ServerSocketChannelUDT;
import com.barchart.udt.nio.SocketChannelUDT;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AFinalizer;

@NotThreadSafe
public class UdtSynchronousChannelServer implements ISynchronousReader<UdtSynchronousChannel> {

    protected final int estimatedMaxMessageSize;
    protected final InetSocketAddress socketAddress;
    private final SocketSynchronousChannelFinalizer finalizer;

    public UdtSynchronousChannelServer(final InetSocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.finalizer = new SocketSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public int getEstimatedMaxMessageSize() {
        return estimatedMaxMessageSize;
    }

    public ServerSocketChannelUDT getServerSocketChannel() {
        return finalizer.serverSocketChannel;
    }

    @Override
    public void open() throws IOException {
        if (finalizer.serverSocketChannel != null) {
            throw new IllegalStateException("Already opened");
        }
        finalizer.serverSocketChannel = newServerSocketChannel();
        finalizer.serverSocketChannel.socketUDT().bind(socketAddress);
        finalizer.serverSocketChannel.socketUDT().listen(256);
        finalizer.serverSocketChannel.configureBlocking(false);
    }

    /**
     * Can be replaced with DATAGRAM
     */
    protected SelectorProviderUDT getUdtProvider() {
        return SelectorProviderUDT.STREAM;
    }

    protected ServerSocketChannelUDT newServerSocketChannel() throws IOException {
        return getUdtProvider().openServerSocketChannel();
    }

    @Override
    public void close() {
        finalizer.close();
    }

    private static final class SocketSynchronousChannelFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private volatile ServerSocketChannelUDT serverSocketChannel;
        private SocketChannelUDT pendingSocketChannel;

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
            final SocketChannelUDT socketChannelCopy = pendingSocketChannel;
            if (socketChannelCopy != null) {
                pendingSocketChannel = null;
                Closeables.closeQuietly(socketChannelCopy);
            }
            final ServerSocketChannelUDT serverSocketChannelCopy = serverSocketChannel;
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
        final ServerSocketChannelUDT serverSocketChannelCopy = finalizer.serverSocketChannel;
        if (serverSocketChannelCopy == null) {
            throw FastEOFException.getInstance("closed");
        }
        finalizer.pendingSocketChannel = serverSocketChannelCopy.accept();
        return finalizer.pendingSocketChannel != null;
    }

    @Override
    public UdtSynchronousChannel readMessage() throws IOException {
        final SocketChannelUDT socketChannel = finalizer.pendingSocketChannel;
        finalizer.pendingSocketChannel = null;
        final UdtSynchronousChannel channel = newUdtSynchronousChannel(socketChannel);
        return channel;
    }

    protected UdtSynchronousChannel newUdtSynchronousChannel(final SocketChannelUDT socketChannel) throws IOException {
        return new UdtSynchronousChannel(this, socketChannel);
    }

    @Override
    public void readFinished() throws IOException {
        //noop
    }

}
