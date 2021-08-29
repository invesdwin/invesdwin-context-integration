package de.invesdwin.context.integration.channel.socket.nio;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public class NioSocketSynchronousReader extends ANioSocketSynchronousChannel
        implements ISynchronousReader<IByteBuffer> {

    private ByteBuffer sizeBuffer;
    private IByteBuffer messageBuffer;
    private int size;

    public NioSocketSynchronousReader(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        sizeBuffer = ByteBuffer.allocateDirect(MESSAGE_INDEX);
        messageBuffer = ByteBuffers.allocateDirectExpandable(estimatedMaxMessageSize);
        size = 0;
    }

    @Override
    public void close() throws IOException {
        if (sizeBuffer != null) {
            sizeBuffer = null;
            messageBuffer = null;
            size = 0;
        }
        super.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (size > 0) {
            return true;
        }
        try {
            final int read = socketChannel.read(sizeBuffer);
            if (read > 0 && sizeBuffer.position() == MESSAGE_INDEX) {
                size = sizeBuffer.getInt(0);
                ByteBuffers.position(sizeBuffer, 0);
            }
            return size > 0;
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        try {
            messageBuffer.putBytesTo(0, socketChannel, size);
            if (ClosedByteBuffer.isClosed(messageBuffer, size)) {
                close();
                throw new EOFException("closed by other side");
            }
            final IByteBuffer message = messageBuffer.sliceTo(size);
            size = 0;
            return message;
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

}
