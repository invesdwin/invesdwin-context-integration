package de.invesdwin.context.integration.channel.stream.server.service;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IStreamSynchronousEndpointService extends ISynchronousChannel {

    int getServiceId();

    String getTopic();

    /**
     * Can return false here if the message can not be immediately written and instead should be put into a queue with
     * potentially creating a copy of the message.
     */
    boolean put(IByteBufferProvider message) throws Exception;

    ISynchronousReader<IByteBufferProvider> subscribe(IStreamSynchronousEndpointServiceListener listener,
            IProperties parameters);

    boolean unsubscribe(IStreamSynchronousEndpointServiceListener listener, IProperties parameters);

    boolean delete(IProperties parameters) throws Exception;

}
