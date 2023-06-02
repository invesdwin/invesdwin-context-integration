package de.invesdwin.context.integration.channel.rpc.client.session;

import java.io.Closeable;

import de.invesdwin.context.integration.channel.rpc.client.SynchronousEndpointClient.ClientMethodInfo;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

public interface ISynchronousEndpointClientSession extends Closeable {

    @Override
    void close();

    ICloseableByteBufferProvider request(ClientMethodInfo methodInfo, IByteBufferProvider request);

}
