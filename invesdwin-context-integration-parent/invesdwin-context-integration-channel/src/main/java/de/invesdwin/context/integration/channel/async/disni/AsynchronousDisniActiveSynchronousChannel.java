package de.invesdwin.context.integration.channel.async.disni;

import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.RdmaActiveEndpointGroup;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.async.disni.endpoint.AsynchronousDisniActiveRdmaEndpoint;
import de.invesdwin.context.integration.channel.async.disni.endpoint.AsynchronousDisniActiveRdmaEndpointFactory;
import de.invesdwin.context.integration.channel.sync.disni.active.ADisniActiveSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.disni.active.endpoint.ADisniActiveRdmaEndpointFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class AsynchronousDisniActiveSynchronousChannel
        extends ADisniActiveSynchronousChannel<AsynchronousDisniActiveRdmaEndpoint> {

    private final IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> handlerFactory;
    private final boolean multipleClientsAllowed;

    public AsynchronousDisniActiveSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize,
            final IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> handlerFactory,
            final boolean multipleClientsAllowed) {
        super(socketAddress, server, estimatedMaxMessageSize);
        this.handlerFactory = handlerFactory;
        this.multipleClientsAllowed = multipleClientsAllowed;
    }

    @Override
    protected ADisniActiveRdmaEndpointFactory<AsynchronousDisniActiveRdmaEndpoint> newRdmaEndpointFactory(
            final RdmaActiveEndpointGroup<AsynchronousDisniActiveRdmaEndpoint> endpointGroup, final int socketSize) {
        return new AsynchronousDisniActiveRdmaEndpointFactory(endpointGroup, socketSize, handlerFactory,
                multipleClientsAllowed);
    }

    @Override
    public void close() {
        super.close();
        try {
            handlerFactory.close();
        } catch (final IOException e) {
            //ignore
        }
    }

}
