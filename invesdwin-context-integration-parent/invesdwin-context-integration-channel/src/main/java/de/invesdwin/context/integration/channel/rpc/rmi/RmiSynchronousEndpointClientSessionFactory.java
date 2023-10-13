package de.invesdwin.context.integration.channel.rpc.rmi;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;

@Immutable
public class RmiSynchronousEndpointClientSessionFactory
        implements ISynchronousEndpointSessionFactory, ISynchronousChannel {

    @Override
    public ISynchronousEndpointSession newSession() {
        return null;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() {}

}
