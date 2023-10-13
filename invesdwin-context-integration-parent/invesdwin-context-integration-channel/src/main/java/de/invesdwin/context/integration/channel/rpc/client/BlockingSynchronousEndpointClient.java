package de.invesdwin.context.integration.channel.rpc.client;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.session.single.SingleplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;

@ThreadSafe
public class BlockingSynchronousEndpointClient<T> extends SynchronousEndpointClient<T> {

    public BlockingSynchronousEndpointClient(final ISynchronousEndpointSessionFactory sessionFactory,
            final Class<T> serviceInterface) {
        super(new SingleplexingSynchronousEndpointClientSessionPool(sessionFactory), serviceInterface);
    }

}
