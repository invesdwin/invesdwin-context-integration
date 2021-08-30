package de.invesdwin.context.integration.channel.jocket;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import jocket.impl.JocketReader;

@NotThreadSafe
public class JocketSynchronousReader extends AJocketSynchronousChannel implements ISynchronousReader<IByteBuffer> {

    private JocketReader reader;
    private IByteBuffer buffer;

    public JocketSynchronousReader(final int port, final boolean server, final int estimatedMaxMessageSize) {
        super(port, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        reader = socket.getReader();
        //old socket would actually slow down with direct buffer because it requires a byte[]
        buffer = ByteBuffers.allocateExpandable(estimatedMaxMessageSize);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
            buffer = null;
        }
        super.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        return true;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        try {
            readFully(buffer.byteArray(), 0, MESSAGE_INDEX);
            final int size = buffer.getInt(SIZE_INDEX);
            readFully(buffer.byteArray(), 0, size);
            if (ClosedByteBuffer.isClosed(buffer, 0, size)) {
                close();
                throw new EOFException("closed by other side");
            }
            return buffer.sliceTo(size);
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

    private void readFully(final byte[] b, final int off, final int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }
        int n = 0;
        while (n < len) {
            final int count = reader.read(b, off + n, len - n);
            if (count < 0) {
                throw new EOFException();
            }
            n += count;
        }
    }

}
