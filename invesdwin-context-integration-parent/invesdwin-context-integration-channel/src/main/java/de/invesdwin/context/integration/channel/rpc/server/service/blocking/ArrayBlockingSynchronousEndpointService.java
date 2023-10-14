package de.invesdwin.context.integration.channel.rpc.server.service.blocking;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.server.blocking.ABlockingSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.blocking.BlockingSynchronousEndpointService;
import de.invesdwin.util.error.Throwables;

@ThreadSafe
public class ArrayBlockingSynchronousEndpointService implements IArrayBlockingSynchronousEndpointService, Closeable {

    private final BlockingSynchronousEndpointService delegate;

    public ArrayBlockingSynchronousEndpointService(final ABlockingSynchronousEndpointServer parent) {
        this.delegate = new BlockingSynchronousEndpointService(parent);
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
