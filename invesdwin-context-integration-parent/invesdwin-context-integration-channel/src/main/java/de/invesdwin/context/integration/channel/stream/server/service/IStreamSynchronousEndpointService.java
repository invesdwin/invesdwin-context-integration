package de.invesdwin.context.integration.channel.stream.server.service;

import java.io.Closeable;
import java.util.Map;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IStreamSynchronousEndpointService extends Closeable {

    int getServiceId();

    String getTopic();

    /**
     * Can return false here if the message can not be immediately written and instead should be put into a queue with
     * potentially creating a copy of the message.
     */
    boolean put(IByteBufferProvider message);

    ISynchronousReader<IByteBufferProvider> subscribe(IStreamSynchronousEndpointServiceListener notificationListener,
            Map<String, String> parameters);

    boolean unsubscribe(IStreamSynchronousEndpointServiceListener notificationListener, Map<String, String> parameters)
            throws Exception;

    boolean delete(Map<String, String> parameters);

    @Override
    void close();

}
