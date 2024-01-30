package de.invesdwin.context.integration.channel.sync.disni.active;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.RdmaServerEndpoint;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.disni.active.endpoint.DisniActiveRdmaEndpoint;
import de.invesdwin.context.integration.channel.sync.disni.active.endpoint.DisniActiveRdmaEndpointFactory;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class DisniActiveSynchronousChannelServer implements ISynchronousReader<DisniActiveSynchronousChannel> {

    protected final SocketAddress socketAddress;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    private final DisniActiveSynchronousChannelFinalizer finalizer;

    public DisniActiveSynchronousChannelServer(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + DisniActiveSynchronousChannel.MESSAGE_INDEX;
        this.finalizer = new DisniActiveSynchronousChannelFinalizer();
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

    public RdmaServerEndpoint<DisniActiveRdmaEndpoint> getServerSocketChannel() {
        return finalizer.serverEndpoint;
    }

    /**
     * Waste a few more cpu cycles for reduced latency with non-blocking.
     */
    protected boolean isBlocking() {
        return false;
    }

    @Override
    public void open() throws IOException {
        if (finalizer.serverEndpoint != null) {
            throw new IllegalStateException("Already opened");
        }

        final boolean blocking = isBlocking();
        finalizer.endpointGroup = new RdmaActiveEndpointGroup<DisniActiveRdmaEndpoint>(
                getConnectTimeout().intValue(FTimeUnit.MILLISECONDS), !blocking, 2, 1, 2);
        final DisniActiveRdmaEndpointFactory factory = new DisniActiveRdmaEndpointFactory(finalizer.endpointGroup,
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

    private static final class DisniActiveSynchronousChannelFinalizer extends AWarningFinalizer {

        private RdmaActiveEndpointGroup<DisniActiveRdmaEndpoint> endpointGroup;
        private volatile RdmaServerEndpoint<DisniActiveRdmaEndpoint> serverEndpoint;
        private DisniActiveRdmaEndpoint pendingEndpoint;

        @Override
        protected void clean() {
            final DisniActiveRdmaEndpoint pendingEndpointCopy = pendingEndpoint;
            if (pendingEndpointCopy != null) {
                pendingEndpoint = null;
                Closeables.closeQuietly(pendingEndpointCopy);
            }
            final RdmaServerEndpoint<DisniActiveRdmaEndpoint> serverEndpointCopy = serverEndpoint;
            if (serverEndpointCopy != null) {
                serverEndpoint = null;
                Closeables.closeQuietly(serverEndpointCopy);
            }
            final RdmaEndpointGroup<DisniActiveRdmaEndpoint> endpointGroupCopy = endpointGroup;
            if (endpointGroupCopy != null) {
                endpointGroup = null;
                Closeables.closeQuietly(endpointGroupCopy);
            }
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
        final RdmaServerEndpoint<DisniActiveRdmaEndpoint> serverEndpointCopy = finalizer.serverEndpoint;
        if (serverEndpointCopy == null) {
            throw FastEOFException.getInstance("closed");
        }
        finalizer.pendingEndpoint = serverEndpointCopy.accept();
        return finalizer.pendingEndpoint != null;
    }

    @Override
    public DisniActiveSynchronousChannel readMessage() throws IOException {
        final DisniActiveRdmaEndpoint socketChannel = finalizer.pendingEndpoint;
        finalizer.pendingEndpoint = null;
        final DisniActiveSynchronousChannel channel = newDisniActiveSynchronousChannel(socketChannel);
        return channel;
    }

    protected DisniActiveSynchronousChannel newDisniActiveSynchronousChannel(
            final DisniActiveRdmaEndpoint socketChannel) throws IOException {
        return new DisniActiveSynchronousChannel(this, socketChannel);
    }

    @Override
    public void readFinished() throws IOException {
        //noop
    }

}
