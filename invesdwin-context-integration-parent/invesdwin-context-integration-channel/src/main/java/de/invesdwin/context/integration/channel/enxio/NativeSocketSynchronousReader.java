package de.invesdwin.context.integration.channel.enxio;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import jnr.enxio.channels.NativeSocketChannel;

@NotThreadSafe
public class NativeSocketSynchronousReader extends ANativeSocketSynchronousChannel
        implements ISynchronousReader<IByteBuffer> {

    private InputStream in;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;

    public NativeSocketSynchronousReader(final InetSocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
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
        if (messageBuffer.position() > 0) {
            return true;
        }
        final int read = socketChannel.read(messageBuffer);
        return read > 0;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        int targetPosition = MESSAGE_INDEX;
        int size = 0;
        //read size
        while (messageBuffer.position() < targetPosition) {
            socketChannel.read(messageBuffer);
        }
        size = buffer.getInt(SIZE_INDEX);
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - messageBuffer.position();
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            readFully(socketChannel, buffer.asByteBuffer(messageBuffer.position(), remaining));
            if (buffer.capacity() != capacityBefore) {
                messageBuffer = buffer.asByteBuffer(0, socketSize);
            }
        }

        ByteBuffers.position(messageBuffer, 0);
        if (ClosedByteBuffer.isClosed(buffer, MESSAGE_INDEX, size)) {
            close();
            throw new EOFException("closed by other side");
        }
        return buffer.slice(MESSAGE_INDEX, size);
    }

    public static void readFully(final NativeSocketChannel src, final java.nio.ByteBuffer byteBuffer)
            throws IOException {
        int remaining = byteBuffer.remaining();
        while (remaining > 0) {
            final int count = src.read(byteBuffer);
            if (count == -1) { // EOF
                break;
            }
            remaining -= count;
        }
        if (remaining > 0) {
            throw ByteBuffers.newPutBytesToEOF();
        }
    }

}
