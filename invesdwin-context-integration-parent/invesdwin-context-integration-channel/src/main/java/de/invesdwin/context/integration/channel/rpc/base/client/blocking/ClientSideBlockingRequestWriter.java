package de.invesdwin.context.integration.channel.rpc.base.client.blocking;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.server.service.blocking.IBufferBlockingEndpointService;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@NotThreadSafe
public class ClientSideBlockingRequestWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final IBufferBlockingEndpointService service;
    private final ClientSideBlockingResponseReader responseReader;

    public ClientSideBlockingRequestWriter(final IBufferBlockingEndpointService service,
            final ClientSideBlockingResponseReader responseReader) {
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
