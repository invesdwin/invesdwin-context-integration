package de.invesdwin.context.integration.channel.rpc.base.server.async;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.async.poll.IPollingQueueProvider;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IAsynchronousEndpointServerHandlerFactory
        extends IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> {

    IPollingQueueProvider getPollingQueueProvider();

    void setPollingQueueProvider(IPollingQueueProvider pollingQueueProvider);

}
