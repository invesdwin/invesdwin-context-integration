package de.invesdwin.context.integration.channel.sync.service.command;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

@Immutable
public class EmptyServiceSynchronousCommand<M> implements IServiceSynchronousCommand<M> {

    public static final int SERVICE = -1;
    public static final int METHOD = -1;
    public static final int SEQUENCE = -1;

    @SuppressWarnings("rawtypes")
    private static final EmptyServiceSynchronousCommand INSTANCE = new EmptyServiceSynchronousCommand<>();

    @SuppressWarnings("unchecked")
    public static <T> EmptyServiceSynchronousCommand<T> getInstance() {
        return INSTANCE;
    }

    @Override
    public int getService() {
        return SERVICE;
    }

    @Override
    public int getMethod() {
        return METHOD;
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
