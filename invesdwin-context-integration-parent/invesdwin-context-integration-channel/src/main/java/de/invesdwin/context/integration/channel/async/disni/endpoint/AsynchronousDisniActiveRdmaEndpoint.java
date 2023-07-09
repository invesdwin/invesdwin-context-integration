package de.invesdwin.context.integration.channel.async.disni.endpoint;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.sync.disni.active.endpoint.ADisniActiveRdmaEndpoint;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class AsynchronousDisniActiveRdmaEndpoint extends ADisniActiveRdmaEndpoint<AsynchronousDisniActiveRdmaEndpoint> {

    private volatile boolean recvFinished;
    private volatile boolean sendFinished;
    private final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler;
    private final boolean multipleClientsAllowed;

    public AsynchronousDisniActiveRdmaEndpoint(
            final RdmaActiveEndpointGroup<AsynchronousDisniActiveRdmaEndpoint> endpointGroup, final RdmaCmId idPriv,
            final boolean serverSide, final int socketSize,
            final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler,
            final boolean multipleClientsAllowed) throws IOException {
        super(endpointGroup, idPriv, serverSide, socketSize);
        this.handler = handler;
        this.multipleClientsAllowed = multipleClientsAllowed;
    }

    @Override
    protected void onSendFinished(final IbvWC wc) {
        sendFinished = true;
    }

    @Override
    protected void onRecvFinished(final IbvWC wc) {
        recvFinished = true;
    }

    public boolean isSendFinished() {
        return sendFinished;
    }

    public void setRecvFinished(final boolean recvFinished) {
        this.recvFinished = recvFinished;
    }

    public boolean isRecvFinished() {
        return recvFinished;
    }

    public void setSendFinished(final boolean sendFinished) {
        this.sendFinished = sendFinished;
    }

}
