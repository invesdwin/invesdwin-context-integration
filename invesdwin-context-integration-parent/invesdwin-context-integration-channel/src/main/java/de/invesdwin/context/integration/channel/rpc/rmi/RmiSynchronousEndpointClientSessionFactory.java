package de.invesdwin.context.integration.channel.rpc.rmi;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;

@Immutable
public class RmiSynchronousEndpointClientSessionFactory implements ISynchronousEndpointSessionFactory {

    @Override
    public ISynchronousEndpointSession newSession() {
        return null;
    }

    @Override
    public void close() {}

}
