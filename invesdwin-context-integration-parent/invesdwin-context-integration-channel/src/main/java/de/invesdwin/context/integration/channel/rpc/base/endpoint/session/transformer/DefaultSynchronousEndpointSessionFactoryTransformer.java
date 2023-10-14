package de.invesdwin.context.integration.channel.rpc.base.endpoint.session.transformer;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.DefaultSynchronousEndpointSessionFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class DefaultSynchronousEndpointSessionFactoryTransformer
        implements ISynchronousEndpointSessionFactoryTransformer {

    @Override
    public ISynchronousEndpointSessionFactory transform(
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> endpointFactory) {
        return new DefaultSynchronousEndpointSessionFactory(endpointFactory);
    }

}
