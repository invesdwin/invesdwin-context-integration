package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

@Immutable
public class SynchronousReaderSupport<M> implements ISynchronousReader<M> {

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {}

    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public M readMessage() throws IOException {
        return null;
    }

    @Override
    public void readFinished() throws IOException {}

}
