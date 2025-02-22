package de.invesdwin.context.integration.channel.async.disni.endpoint;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.RdmaCmId;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.disni.active.endpoint.ADisniActiveRdmaEndpointFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class AsynchronousDisniActiveRdmaEndpointFactory
        extends ADisniActiveRdmaEndpointFactory<AsynchronousDisniActiveRdmaEndpoint> implements ISynchronousChannel {

    private final IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> handlerFactory;
    private final boolean multipleClientsAllowed;

    public AsynchronousDisniActiveRdmaEndpointFactory(
            final RdmaActiveEndpointGroup<AsynchronousDisniActiveRdmaEndpoint> endpointGroup, final int bufferSize,
            final IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> handlerFactory,
            final boolean multipleClientsAllowed) {
        super(endpointGroup, bufferSize);
        this.handlerFactory = handlerFactory;
        this.multipleClientsAllowed = multipleClientsAllowed;
    }

    @Override
    public void open() throws IOException {
        handlerFactory.open();
    }

    @Override
    public void close() throws IOException {
        handlerFactory.close();
    }

    @Override
    public AsynchronousDisniActiveRdmaEndpoint createEndpoint(final RdmaCmId id, final boolean serverSide)
            throws IOException {
        return new AsynchronousDisniActiveRdmaEndpoint(endpointGroup, id, serverSide, bufferSize,
                handlerFactory.newHandler(), multipleClientsAllowed);
    }

}
