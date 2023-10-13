package de.invesdwin.context.integration.channel.rpc.rmi.service;

import java.io.Closeable;
import java.io.IOException;
import java.rmi.RemoteException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.rmi.RmiSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.blocking.BlockingSynchronousEndpointService;
import de.invesdwin.util.error.Throwables;

@ThreadSafe
public class RmiSynchronousEndpointService implements IRmiSynchronousEndpointService, Closeable {

    private final BlockingSynchronousEndpointService delegate;

    public RmiSynchronousEndpointService(final RmiSynchronousEndpointServer parent) {
        this.delegate = new BlockingSynchronousEndpointService(parent);
    }

    @Override
    public byte[] call(final byte[] request) throws RemoteException {
        try {
            return delegate.call(request);
        } catch (final Throwable t) {
            throw new RemoteException(Throwables.getFullStackTrace(t));
        }
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

}
