package de.invesdwin.context.integration.channel.jocket;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public class JocketSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private final JocketChannel channel;
    private InputStream in;
    private IByteBuffer buffer;

    public JocketSynchronousReader(final JocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        in = channel.getSocket().getInputStream();
        //old socket would actually slow down with direct buffer because it requires a byte[]
        buffer = ByteBuffers.allocateExpandable(channel.getEstimatedMaxMessageSize());
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
            buffer = null;
        }
        channel.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        return true;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        try {
            buffer.putBytesTo(0, in, JocketChannel.MESSAGE_INDEX);
            final int size = buffer.getInt(JocketChannel.SIZE_INDEX);
            buffer.putBytesTo(0, in, size);
            if (ClosedByteBuffer.isClosed(buffer, 0, size)) {
                close();
                throw new EOFException("closed by other side");
            }
            return buffer.sliceTo(size);
        } catch (final IOException e) {
            throw channel.newEofException(e);
        }
    }

}
