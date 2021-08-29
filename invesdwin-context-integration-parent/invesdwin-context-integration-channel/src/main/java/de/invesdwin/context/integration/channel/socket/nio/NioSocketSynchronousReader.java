package de.invesdwin.context.integration.channel.socket.nio;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
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

    private InputStream in;
    private IByteBuffer buffer;
    private ByteBuffer messageBuffer;

    public NioSocketSynchronousReader(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        in = socket.getInputStream();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(estimatedMaxMessageSize);
        messageBuffer = buffer.asByteBuffer(0, socketSize);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
            buffer = null;
            messageBuffer = null;
        }
        super.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            //this is a lot faster then directly reading on the channel
            return in.available() >= MESSAGE_INDEX;
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        ByteBuffers.position(messageBuffer, 0);
        int targetPosition = MESSAGE_INDEX;
        int size = 0;
        //read size
        while (true) {
            final int read = socketChannel.read(messageBuffer);
            if (read < 0) {
                throw new EOFException("closed by other side");
            }
            if (read > 0 && messageBuffer.position() >= targetPosition) {
                size = buffer.getInt(SIZE_INDEX);
                targetPosition += size;
                break;
            }
        }
        //read message if not complete yet
        final int remaining = targetPosition - messageBuffer.position();
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            buffer.putBytesTo(messageBuffer.position(), socketChannel, remaining);
            if (buffer.capacity() != capacityBefore) {
                //update reference to underlying storage
                messageBuffer = buffer.asByteBuffer(0, socketSize);
            }
        }

        if (ClosedByteBuffer.isClosed(buffer, MESSAGE_INDEX, size)) {
            close();
            throw new EOFException("closed by other side");
        }
        return buffer.slice(MESSAGE_INDEX, size);
    }

}
