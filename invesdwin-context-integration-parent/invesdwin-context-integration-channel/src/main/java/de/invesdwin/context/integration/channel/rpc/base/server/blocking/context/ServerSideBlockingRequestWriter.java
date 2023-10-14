package de.invesdwin.context.integration.channel.rpc.base.server.blocking.context;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class ServerSideBlockingRequestWriter implements ISynchronousWriter<IByteBufferProvider> {

    private IByteBufferProvider message;

    public IByteBufferProvider getMessage() {
        return message;
    }

    public void setMessage(final IByteBufferProvider message) {
        this.message = message;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        message = null;
    }

    @Override
    public boolean writeReady() throws IOException {
        return message == null;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        this.message = message;
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
