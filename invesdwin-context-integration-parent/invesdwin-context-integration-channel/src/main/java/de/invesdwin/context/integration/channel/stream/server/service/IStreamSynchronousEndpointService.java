package de.invesdwin.context.integration.channel.stream.server.service;

import java.io.Closeable;
import java.util.Map;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IStreamSynchronousEndpointService extends Closeable {

    int getServiceId();

    String getTopic();

    ISynchronousWriter<IByteBufferProvider> getSharedWriter();

    ISynchronousReader<IByteBufferProvider> subscribe(Runnable notificationListener, Map<String, String> properties);

    boolean unsubscribe(Runnable notificationListener, Map<String, String> properties);

    void delete();

    @Override
    void close();

}
