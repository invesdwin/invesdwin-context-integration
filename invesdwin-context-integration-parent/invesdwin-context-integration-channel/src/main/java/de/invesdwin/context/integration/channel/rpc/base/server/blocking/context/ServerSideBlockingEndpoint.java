package de.invesdwin.context.integration.channel.rpc.base.server.blocking.context;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.reference.SimpleReferenceSynchronousReader;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class ServerSideBlockingEndpoint implements ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> {

    private final SimpleReferenceSynchronousReader<IByteBufferProvider> reader;
    private final ServerSideBlockingRequestWriter writer;

    public ServerSideBlockingEndpoint() {
        this.reader = new SimpleReferenceSynchronousReader<>(new MutableReference<>());
        this.writer = new ServerSideBlockingRequestWriter();
    }

    @Override
    public SimpleReferenceSynchronousReader<IByteBufferProvider> getReader() {
        return reader;
    }

    @Override
    public ServerSideBlockingRequestWriter getWriter() {
        return writer;
    }

}
