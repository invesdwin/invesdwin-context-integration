package de.invesdwin.context.integration.channel.rpc.base.endpoint.session;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.system.Processes;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@ThreadSafe
public class DefaultSynchronousEndpointSessionFactory implements ISynchronousEndpointSessionFactory {

    private static final AtomicLong NEXT_FACTORY_ID = new AtomicLong();
    private final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> endpointFactory;
    private final AtomicLong nextId = new AtomicLong();
    private final long factoryId;

    public DefaultSynchronousEndpointSessionFactory(
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> endpointFactory) {
        this.endpointFactory = endpointFactory;
        this.factoryId = NEXT_FACTORY_ID.incrementAndGet();
    }

    @Override
    public ISynchronousEndpointSession newSession() {
        final String sessionId = Processes.getProcessId() + "_" + factoryId + "_" + nextId.incrementAndGet();
        final ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint = endpointFactory.newEndpoint();
        return new DefaultSynchronousEndpointSession(sessionId, endpoint, null);
    }

    @Override
    public void close() {
        endpointFactory.close();
    }

}
