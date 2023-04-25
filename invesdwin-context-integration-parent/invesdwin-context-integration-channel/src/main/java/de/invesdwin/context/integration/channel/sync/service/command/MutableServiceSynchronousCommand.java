package de.invesdwin.context.integration.channel.sync.service.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class MutableServiceSynchronousCommand<M> implements IServiceSynchronousCommand<M> {

    protected int service;
    protected int method;
    protected int sequence;
    protected M message;

    @Override
    public int getService() {
        return service;
    }

    public void setService(final int service) {
        this.service = service;
    }

    @Override
    public int getMethod() {
        return method;
    }

    public void setMethod(final int method) {
        this.method = method;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    public void setSequence(final int sequence) {
        this.sequence = sequence;
    }

    @Override
    public M getMessage() {
        return message;
    }

    public void setMessage(final M message) {
        this.message = message;
    }

    @Override
    public void close() throws IOException {
        message = null; //free memory
    }

}
