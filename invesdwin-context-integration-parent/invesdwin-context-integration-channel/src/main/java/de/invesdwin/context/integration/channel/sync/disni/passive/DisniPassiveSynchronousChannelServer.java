package de.invesdwin.context.integration.channel.sync.disni.passive;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.RdmaPassiveEndpointGroup;
import com.ibm.disni.RdmaServerEndpoint;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.disni.passive.endpoint.DisniPassiveRdmaEndpoint;
import de.invesdwin.context.integration.channel.sync.disni.passive.endpoint.DisniPassiveRdmaEndpointFactory;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class DisniPassiveSynchronousChannelServer implements ISynchronousReader<DisniPassiveSynchronousChannel> {

    protected final SocketAddress socketAddress;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    private final DisniPassiveSynchronousChannelFinalizer finalizer;

    public DisniPassiveSynchronousChannelServer(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + DisniPassiveSynchronousChannel.MESSAGE_INDEX;
        this.finalizer = new DisniPassiveSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public int getEstimatedMaxMessageSize() {
        return estimatedMaxMessageSize;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public RdmaServerEndpoint<DisniPassiveRdmaEndpoint> getServerSocketChannel() {
        return finalizer.serverEndpoint;
    }

    @Override
    public void open() throws IOException {
        if (finalizer.serverEndpoint != null) {
            throw new IllegalStateException("Already opened");
        }

        finalizer.endpointGroup = new RdmaPassiveEndpointGroup<>(getConnectTimeout().intValue(FTimeUnit.MILLISECONDS),
                2, 1, 2);
        final DisniPassiveRdmaEndpointFactory factory = new DisniPassiveRdmaEndpointFactory(finalizer.endpointGroup,
                socketSize);
        finalizer.endpointGroup.init(factory);
        finalizer.serverEndpoint = finalizer.endpointGroup.createServerEndpoint();
        try {
            finalizer.serverEndpoint.bind(socketAddress, 1);
        } catch (final Throwable t) {
            throw new IOException(t);
        }
    }

    protected Duration getConnectTimeout() {
        return SynchronousChannels.DEFAULT_CONNECT_TIMEOUT;
    }

    protected ServerSocketChannel newServerEndpoint() throws IOException {
        return ServerSocketChannel.open();
    }

    @Override
    public void close() {
        finalizer.close();
    }

    private static final class DisniPassiveSynchronousChannelFinalizer extends AFinalizer {

        private final Exception initStackTrace;
        private RdmaPassiveEndpointGroup<DisniPassiveRdmaEndpoint> endpointGroup;
        private volatile RdmaServerEndpoint<DisniPassiveRdmaEndpoint> serverEndpoint;
        private DisniPassiveRdmaEndpoint pendingEndpoint;

        protected DisniPassiveSynchronousChannelFinalizer() {
            if (Throwables.isDebugStackTraceEnabled()) {
                initStackTrace = new Exception();
                initStackTrace.fillInStackTrace();
            } else {
                initStackTrace = null;
            }
        }

        @Override
        protected void clean() {
            final DisniPassiveRdmaEndpoint socketChannelCopy = pendingEndpoint;
            if (socketChannelCopy != null) {
                pendingEndpoint = null;
                Closeables.closeQuietly(socketChannelCopy);
            }
            final RdmaServerEndpoint<DisniPassiveRdmaEndpoint> serverSocketChannelCopy = serverEndpoint;
            if (serverSocketChannelCopy != null) {
                serverEndpoint = null;
                Closeables.closeQuietly(serverSocketChannelCopy);
            }
            final RdmaEndpointGroup<DisniPassiveRdmaEndpoint> endpointGroupCopy = endpointGroup;
            if (endpointGroupCopy != null) {
                endpointGroup = null;
                Closeables.closeQuietly(endpointGroupCopy);
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
            return serverEndpoint == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

    @Override
    public boolean hasNext() throws IOException {
        if (finalizer.pendingEndpoint != null) {
            return true;
        }
        final RdmaServerEndpoint<DisniPassiveRdmaEndpoint> serverSocketChannelCopy = finalizer.serverEndpoint;
        if (serverSocketChannelCopy == null) {
            throw FastEOFException.getInstance("closed");
        }
        finalizer.pendingEndpoint = serverSocketChannelCopy.accept();
        return finalizer.pendingEndpoint != null;
    }

    @Override
    public DisniPassiveSynchronousChannel readMessage() throws IOException {
        final DisniPassiveRdmaEndpoint socketChannel = finalizer.pendingEndpoint;
        finalizer.pendingEndpoint = null;
        final DisniPassiveSynchronousChannel channel = newDisniPassiveSynchronousChannel(socketChannel);
        return channel;
    }

    protected DisniPassiveSynchronousChannel newDisniPassiveSynchronousChannel(
            final DisniPassiveRdmaEndpoint socketChannel) throws IOException {
        return new DisniPassiveSynchronousChannel(this, socketChannel);
    }

    @Override
    public void readFinished() throws IOException {
        //noop
    }

}
