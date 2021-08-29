package de.invesdwin.context.integration.channel.socket.udp;

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
public class DatagramSocketSynchronousReader extends ADatagramSocketSynchronousChannel
        implements ISynchronousReader<IByteBuffer> {

    protected IByteBuffer buffer;
    protected ByteBuffer messageBuffer;

    public DatagramSocketSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        super(socketAddress, true, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = buffer.asByteBuffer(0, socketSize);
    }

    @Override
    public void close() throws IOException {
        super.close();
        buffer = null;
        messageBuffer = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (messageBuffer.position() > 0) {
            return true;
        }
        final SocketAddress client = socketChannel.receive(messageBuffer);
        return client != null;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        int targetPosition = MESSAGE_INDEX;
        int size = 0;
        //read size
        while (messageBuffer.position() < targetPosition) {
            socketChannel.receive(messageBuffer);
        }
        size = buffer.getInt(SIZE_INDEX);
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - messageBuffer.position();
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            buffer.putBytesTo(messageBuffer.position(), socketChannel, remaining);
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

}
