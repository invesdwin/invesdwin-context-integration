package de.invesdwin.context.integration.channel.pipe;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public class PipeSynchronousReader extends APipeSynchronousChannel implements ISynchronousReader<IByteBuffer> {

    private FileInputStream in;
    private IByteBuffer buffer;

    public PipeSynchronousReader(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        in = new FileInputStream(file);
        buffer = ByteBuffers.allocateExpandable(estimatedMaxMessageSize);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
            buffer = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            return in.available() >= MESSAGE_INDEX;
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        try {
            buffer.putBytesTo(0, in, MESSAGE_INDEX);
            final int size = buffer.getInt(SIZE_INDEX);
            buffer.putBytesTo(0, in, size);
            if (ClosedByteBuffer.isClosed(buffer, size)) {
                close();
                throw new EOFException("closed by other side");
            }
            return buffer.sliceTo(size);
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

}
