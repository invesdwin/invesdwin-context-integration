package de.invesdwin.context.integration.channel.sync.jucx;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpRequest;

@ThreadSafe
public class ErrorUcxCallback extends UcxCallback {

    private volatile boolean finished;
    private volatile String error;

    public ErrorUcxCallback reset() {
        finished = false;
        error = null;
        return this;
    }

    public boolean isFinished() {
        return finished;
    }

    public String getError() {
        return error;
    }

    @Override
    public void onSuccess(final UcpRequest request) {
        finished = true;
        error = null;
    }

    @Override
    public void onError(final int ucsStatus, final String errorMsg) {
        finished = true;
        error = ucsStatus + ": " + errorMsg;
    }

    public void maybeThrow() throws IOException {
        final String errorCopy = error;
        if (errorCopy != null) {
            throw new IOException(errorCopy);
        }
    }
}