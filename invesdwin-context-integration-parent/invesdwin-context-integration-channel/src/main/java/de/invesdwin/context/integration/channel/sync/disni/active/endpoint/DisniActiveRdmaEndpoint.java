package de.invesdwin.context.integration.channel.sync.disni.active.endpoint;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;

@NotThreadSafe
public class DisniActiveRdmaEndpoint extends ADisniActiveRdmaEndpoint<DisniActiveRdmaEndpoint> {

    private volatile boolean recvFinished;
    private volatile boolean sendFinished;

    public DisniActiveRdmaEndpoint(
            final RdmaActiveEndpointGroup<DisniActiveRdmaEndpoint> endpointGroup, final RdmaCmId idPriv,
            final boolean serverSide, final int socketSize) throws IOException {
        super(endpointGroup, idPriv, serverSide, socketSize);
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
