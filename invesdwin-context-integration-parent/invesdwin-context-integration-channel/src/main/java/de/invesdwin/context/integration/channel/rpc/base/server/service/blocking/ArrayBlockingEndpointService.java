package de.invesdwin.context.integration.channel.rpc.base.server.service.blocking;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.server.blocking.ABlockingEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.blocking.BlockingEndpointService;
import de.invesdwin.util.error.Throwables;

@ThreadSafe
public class ArrayBlockingEndpointService implements IArrayBlockingEndpointService, Closeable {

    private final BlockingEndpointService delegate;

    public ArrayBlockingEndpointService(final ABlockingEndpointServer parent) {
        this.delegate = new BlockingEndpointService(parent);
    }

    @Override
    public byte[] call(final byte[] request) throws IOException {
        try {
            return delegate.call(request);
        } catch (final Throwable t) {
            throw new IOException(Throwables.getFullStackTrace(t));
        }
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

}
