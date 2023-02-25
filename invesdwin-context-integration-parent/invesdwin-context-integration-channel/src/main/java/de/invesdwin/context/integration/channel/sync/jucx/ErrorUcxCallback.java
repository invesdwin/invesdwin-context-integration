package de.invesdwin.context.integration.channel.sync.jucx;

import java.io.IOException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpRequest;

@ThreadSafe
public class ErrorUcxCallback extends UcxCallback {

    @GuardedBy("this")
    private boolean finished;
    @GuardedBy("this")
    private String error;

    public ErrorUcxCallback reset() {
        finished = false;
        error = null;
        return this;
    }

    public synchronized boolean isFinished() {
        return finished;
    }

    public synchronized String getError() {
        return error;
    }

    @Override
    public synchronized void onSuccess(final UcpRequest request) {
        finished = true;
        error = null;
    }

    @Override
    public synchronized void onError(final int ucsStatus, final String errorMsg) {
        finished = true;
        error = ucsStatus + ": " + errorMsg;
    }

    public synchronized ErrorUcxCallback maybeThrowAndReset() throws IOException {
        maybeThrow();
        return reset();
    }

    public synchronized void maybeThrow() throws IOException {
        final String errorCopy = error;
        if (errorCopy != null) {
            throw new IOException(errorCopy);
        }
    }
}