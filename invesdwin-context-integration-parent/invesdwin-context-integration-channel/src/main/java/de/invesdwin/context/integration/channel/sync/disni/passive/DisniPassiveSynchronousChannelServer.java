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
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
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
        this.socketSize = newSocketSize(estimatedMaxMessageSize);
        this.finalizer = new DisniPassiveSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    protected int newSocketSize(final int estimatedMaxMessageSize) {
        return estimatedMaxMessageSize + DisniPassiveSynchronousChannel.MESSAGE_INDEX;
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

    private static final class DisniPassiveSynchronousChannelFinalizer extends AWarningFinalizer {

        private RdmaPassiveEndpointGroup<DisniPassiveRdmaEndpoint> endpointGroup;
        private volatile RdmaServerEndpoint<DisniPassiveRdmaEndpoint> serverEndpoint;
        private DisniPassiveRdmaEndpoint pendingEndpoint;

        @Override
        protected void clean() {
            final DisniPassiveRdmaEndpoint pendingEndpointCopy = pendingEndpoint;
            if (pendingEndpointCopy != null) {
                pendingEndpoint = null;
                Closeables.closeQuietly(pendingEndpointCopy);
            }
            final RdmaServerEndpoint<DisniPassiveRdmaEndpoint> serverEndpointCopy = serverEndpoint;
            if (serverEndpointCopy != null) {
                serverEndpoint = null;
                Closeables.closeQuietly(serverEndpointCopy);
            }
            final RdmaEndpointGroup<DisniPassiveRdmaEndpoint> endpointGroupCopy = endpointGroup;
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
        final RdmaServerEndpoint<DisniPassiveRdmaEndpoint> serverEndpointCopy = finalizer.serverEndpoint;
        if (serverEndpointCopy == null) {
            throw FastEOFException.getInstance("closed");
        }
        finalizer.pendingEndpoint = serverEndpointCopy.accept();
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
