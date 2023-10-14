package de.invesdwin.context.integration.channel.rpc.client.blocking;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.server.service.blocking.IBufferBlockingEndpointService;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class ClientSideBlockingEndpoint
        implements ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> {

    private final ClientSideBlockingResponseReader reader;
    private final ClientSideBlockingRequestWriter writer;

    public ClientSideBlockingEndpoint(final IBufferBlockingEndpointService service) {
        this.reader = new ClientSideBlockingResponseReader();
        this.writer = new ClientSideBlockingRequestWriter(service, reader);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public ISynchronousReader<IByteBufferProvider> getReader() {
        return (ISynchronousReader) reader;
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> getWriter() {
        return writer;
    }

}
