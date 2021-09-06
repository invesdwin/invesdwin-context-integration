package de.invesdwin.context.integration.channel.sync.command;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class ImmutableSynchronousCommand<M> implements ISynchronousCommand<M> {

    private final int type;
    private final int sequence;
    private final M message;

    public ImmutableSynchronousCommand(final int type, final int sequence, final M message) {
        this.type = type;
        this.sequence = sequence;
        this.message = message;
    }

    private ImmutableSynchronousCommand(final ISynchronousCommand<M> response) {
        this(response.getType(), response.getSequence(), response.getMessage());
    }

    @Override
    public int getType() {
        return type;
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

    public static <T> ImmutableSynchronousCommand<T> valueOf(final ISynchronousCommand<T> response) {
        if (response == null) {
            return null;
        } else if (response instanceof ImmutableSynchronousCommand) {
            return (ImmutableSynchronousCommand<T>) response;
        } else {
            return new ImmutableSynchronousCommand<T>(response);
        }
    }

}
