package de.invesdwin.context.integration.channel.async;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

@Immutable
public class AsynchronousHandlerFactorySupport<I, O> implements IAsynchronousHandlerFactory<I, O> {

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {}

    @Override
    public IAsynchronousHandler<I, O> newHandler() {
        return null;
    }

}
