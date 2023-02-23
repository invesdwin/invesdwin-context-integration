package de.invesdwin.context.integration.channel.sync.ucx.jucx;

import javax.annotation.concurrent.NotThreadSafe;

import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;

import de.invesdwin.util.concurrent.loop.ASpinWait;

@NotThreadSafe
public class UcpRequestSpinWait extends ASpinWait {

    private UcpWorker worker;
    private UcpRequest request;

    public void init(final UcpWorker worker, final UcpRequest request) {
        this.worker = worker;
        this.request = request;
    }

    public void clear() {
        request = null;
        worker = null;
    }

    @Override
    public boolean isConditionFulfilled() throws Exception {
        worker.progressRequest(request);
        return request.isCompleted();
    }

}
