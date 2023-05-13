package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

@Immutable
public class SynchronousWriterSupport<M> implements ISynchronousWriter<M> {

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {}

    @Override
    public boolean writeReady() throws IOException {
        return false;
    }

    @Override
    public void write(final M message) throws IOException {}

    @Override
    public boolean writeFlushed() throws IOException {
        return false;
    }

}
