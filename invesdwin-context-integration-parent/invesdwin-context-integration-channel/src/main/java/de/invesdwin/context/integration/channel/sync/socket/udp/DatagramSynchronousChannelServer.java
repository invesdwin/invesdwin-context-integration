package de.invesdwin.context.integration.channel.sync.socket.udp;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.channels.ServerSocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AFinalizer;

@NotThreadSafe
public class DatagramSynchronousChannelServer implements ISynchronousReader<DatagramSynchronousChannel> {

    protected final int estimatedMaxMessageSize;
    protected final SocketAddress socketAddress;
    private final DatagramSynchronousChannelFinalizer finalizer;

    public DatagramSynchronousChannelServer(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.finalizer = new DatagramSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public int getEstimatedMaxMessageSize() {
        return estimatedMaxMessageSize;
    }

    public ServerDatagramChannel getServerDatagramChannel() {
        return finalizer.serverDatagramChannel;
    }

    @Override
    public void open() throws IOException {
        if (finalizer.serverDatagramChannel != null) {
            throw new IllegalStateException("Already opened");
        }
        finalizer.serverDatagramChannel = newServerDatagramChannel();
        finalizer.serverDatagramChannel.bind(socketAddress);
        finalizer.serverDatagramChannel.configureBlocking(false);
    }

    protected ServerDatagramChannel newServerDatagramChannel() throws IOException {
        return ServerDatagramChannel.open();
    }

    @Override
    public void close() {
        finalizer.close();
    }

    private static final class DatagramSynchronousChannelFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private volatile ServerDatagramChannel serverDatagramChannel;
        private DatagramChannel pendingDatagramChannel;

        protected DatagramSynchronousChannelFinalizer() {
            if (Throwables.isDebugStackTraceEnabled()) {
                initStackTrace = new Exception();
                initStackTrace.fillInStackTrace();
            } else {
                initStackTrace = null;
            }
        }

        @Override
        protected void clean() {
            final DatagramChannel datagramChannelCopy = pendingDatagramChannel;
            if (datagramChannelCopy != null) {
                pendingDatagramChannel = null;
                Closeables.closeQuietly(datagramChannelCopy);
            }
            final ServerSocketChannel serverDatagramChannelCopy = serverDatagramChannel;
            if (serverDatagramChannelCopy != null) {
                serverDatagramChannel = null;
                Closeables.closeQuietly(serverDatagramChannelCopy);
            }
        }

        @Override
        protected void onRun() {
            String warning = "Finalizing unclosed " + DatagramSynchronousChannel.class.getSimpleName();
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
            return serverDatagramChannel == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

    @Override
    public boolean hasNext() throws IOException {
        if (finalizer.pendingDatagramChannel != null) {
            return true;
        }
        final ServerDatagramChannel serverDatagramChannelCopy = finalizer.serverDatagramChannel;
        if (serverDatagramChannelCopy == null) {
            throw FastEOFException.getInstance("closed");
        }
        finalizer.pendingDatagramChannel = serverDatagramChannelCopy.accept();
        return finalizer.pendingDatagramChannel != null;
    }

    @Override
    public DatagramSynchronousChannel readMessage() throws IOException {
        final DatagramChannel datagramChannel = finalizer.pendingDatagramChannel;
        finalizer.pendingDatagramChannel = null;
        final DatagramSynchronousChannel channel = newDatagramSynchronousChannel(datagramChannel);
        return channel;
    }

    protected DatagramSynchronousChannel newDatagramSynchronousChannel(final DatagramChannel datagramChannel)
            throws IOException {
        return new DatagramSynchronousChannel(this, datagramChannel);
    }

    @Override
    public void readFinished() throws IOException {
        //noop
    }

}
