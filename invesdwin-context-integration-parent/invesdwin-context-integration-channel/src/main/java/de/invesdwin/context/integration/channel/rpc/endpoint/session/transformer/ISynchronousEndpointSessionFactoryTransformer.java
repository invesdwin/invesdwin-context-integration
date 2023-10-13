package de.invesdwin.context.integration.channel.rpc.endpoint.session.transformer;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface ISynchronousEndpointSessionFactoryTransformer {

    ISynchronousEndpointSessionFactory transform(
            ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> endpointFactory);

}
