package de.invesdwin.context.integration.channel.socket;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public class SocketSynchronousReader extends ASocketSynchronousChannel implements ISynchronousReader<IByteBuffer> {

    private InputStream in;
    private IByteBuffer buffer;

    public SocketSynchronousReader(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        in = socket.getInputStream();
        buffer = ByteBuffers.allocateExpandable(estimatedMaxMessageSize);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
            buffer = null;
        }
        super.close();
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
            buffer.putBytesTo(0, socket.getChannel(), MESSAGE_INDEX);
            final int size = buffer.getInt(SIZE_INDEX);
            buffer.putBytesTo(0, socket.getChannel(), size);
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
