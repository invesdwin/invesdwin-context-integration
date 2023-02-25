package de.invesdwin.context.integration.channel.sync.jucx;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.openucx.jucx.ucp.UcpRequest;

import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.time.duration.Duration;

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

    public void waitForRequest(final UcpRequest request, final Duration timeout) throws IOException {
        try {
            init(request);
            awaitFulfill(System.nanoTime(), timeout);
        } catch (final IOException e) {
            throw e;
        } catch (final Throwable t) {
            throw new IOException(t);
        }
    }

    @Override
    public boolean isConditionFulfilled() throws Exception {
        channel.getUcpWorker().progress();
        channel.getUcpWorker().progressRequest(request);
        channel.getErrorUcxCallback().maybeThrow();
        return request.isCompleted();
    }

}
