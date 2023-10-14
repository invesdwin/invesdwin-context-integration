package de.invesdwin.context.integration.channel.rpc.client.blocking;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.server.service.blocking.IBufferBlockingSynchronousEndpointService;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@NotThreadSafe
public class ClientSideBlockingSynchronousRequestWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final IBufferBlockingSynchronousEndpointService service;
    private final ClientSideBlockingSynchronousResponseReader responseReader;

    public ClientSideBlockingSynchronousRequestWriter(final IBufferBlockingSynchronousEndpointService service,
            final ClientSideBlockingSynchronousResponseReader responseReader) {
        this.service = service;
        this.responseReader = responseReader;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {}

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final ICloseableByteBufferProvider response = service.call(message);
        responseReader.setMessage(response);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
