package de.invesdwin.context.integration.channel.rpc.client.blocking;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.server.service.blocking.IBufferBlockingSynchronousEndpointService;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class ClientSideBlockingSynchronousEndpoint
        implements ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> {

    private final ClientSideBlockingSynchronousResponseReader reader;
    private final ClientSideBlockingSynchronousRequestWriter writer;

    public ClientSideBlockingSynchronousEndpoint(final IBufferBlockingSynchronousEndpointService service) {
        this.reader = new ClientSideBlockingSynchronousResponseReader();
        this.writer = new ClientSideBlockingSynchronousRequestWriter(service, reader);
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
