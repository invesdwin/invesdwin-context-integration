package de.invesdwin.context.integration.channel.sync.service.command;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class ImmutableServiceSynchronousCommand<M> implements IServiceSynchronousCommand<M> {

    private final int service;
    private final int method;
    private final int sequence;
    private final M message;

    public ImmutableServiceSynchronousCommand(final int service, final int method, final int sequence,
            final M message) {
        this.service = service;
        this.method = method;
        this.sequence = sequence;
        this.message = message;
    }

    private ImmutableServiceSynchronousCommand(final IServiceSynchronousCommand<M> response) {
        this(response.getService(), response.getMethod(), response.getSequence(), response.getMessage());
    }

    @Override
    public int getService() {
        return service;
    }

    @Override
    public int getMethod() {
        return method;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    @Override
    public M getMessage() {
        return message;
    }

    @Override
    public void close() throws IOException {
        //noop
    }

    public static <T> ImmutableServiceSynchronousCommand<T> valueOf(final IServiceSynchronousCommand<T> response) {
        if (response == null) {
            return null;
        } else if (response instanceof ImmutableServiceSynchronousCommand) {
            return (ImmutableServiceSynchronousCommand<T>) response;
        } else {
            return new ImmutableServiceSynchronousCommand<T>(response);
        }
    }

}
