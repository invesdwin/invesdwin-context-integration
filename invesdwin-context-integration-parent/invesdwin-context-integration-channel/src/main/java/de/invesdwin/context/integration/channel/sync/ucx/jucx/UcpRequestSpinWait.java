package de.invesdwin.context.integration.channel.sync.ucx.jucx;

import javax.annotation.concurrent.NotThreadSafe;

import org.openucx.jucx.ucp.UcpRequest;

import de.invesdwin.util.concurrent.loop.ASpinWait;

@NotThreadSafe
public class UcpRequestSpinWait extends ASpinWait {

    private final IJucxSynchronousChannel channel;
    private UcpRequest request;

    public UcpRequestSpinWait(final IJucxSynchronousChannel channel) {
        this.channel = channel;
    }

    public void init(final UcpRequest request) {
        this.request = request;
    }

    @Override
    public boolean isConditionFulfilled() throws Exception {
        channel.getUcpWorker().progress();
        channel.getUcpWorker().progressRequest(request);
        channel.getErrorUcxCallback().maybeThrow();
        return request.isCompleted();
    }

}
