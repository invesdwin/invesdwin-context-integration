package de.invesdwin.context.integration.channel.rpc.client.blocking;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@NotThreadSafe
public class ClientSideBlockingSynchronousResponseReader implements ISynchronousReader<ICloseableByteBufferProvider> {

    private ICloseableByteBufferProvider message;
    private ICloseableByteBufferProvider closeMessage;

    public void setMessage(final ICloseableByteBufferProvider message) {
        this.message = message;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        message = null;
        if (closeMessage != null) {
            closeMessage.close();
            closeMessage = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return message != null;
    }

    @Override
    public ICloseableByteBufferProvider readMessage() throws IOException {
        final ICloseableByteBufferProvider readMessage = message;
        closeMessage = readMessage;
        message = null;
        return readMessage;
    }

    @Override
    public void readFinished() {
        closeMessage.close();
        closeMessage = null;
    }

}
