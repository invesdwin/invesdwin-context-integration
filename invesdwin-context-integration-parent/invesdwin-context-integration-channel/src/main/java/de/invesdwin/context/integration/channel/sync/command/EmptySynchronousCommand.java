package de.invesdwin.context.integration.channel.sync.command;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

@Immutable
public class EmptySynchronousCommand<M> implements ISynchronousCommand<M> {

    public static final int TYPE = -1;
    public static final int SEQUENCE = -1;

    @SuppressWarnings("rawtypes")
    private static final EmptySynchronousCommand INSTANCE = new EmptySynchronousCommand<>();

    @SuppressWarnings("unchecked")
    public static <T> EmptySynchronousCommand<T> getInstance() {
        return INSTANCE;
    }

    @Override
    public int getType() {
        return TYPE;
    }

    @Override
    public int getSequence() {
        return SEQUENCE;
    }

    @Override
    public M getMessage() {
        return null;
    }

    @Override
    public void close() throws IOException {
        //noop
    }

}
