package de.invesdwin.context.integration.channel.rpc.client.session;

import java.io.Closeable;

import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

public interface ISynchronousEndpointClientSession extends Closeable {

    @Override
    void close();

    ICloseableByteBufferProvider request(int serviceId, int methodId, IByteBufferProvider request);

}
