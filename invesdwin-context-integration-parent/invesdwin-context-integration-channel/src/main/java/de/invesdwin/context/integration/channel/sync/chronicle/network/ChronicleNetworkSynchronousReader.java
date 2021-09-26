package de.invesdwin.context.integration.channel.sync.chronicle.network;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.chronicle.network.type.ChronicleSocketChannelType;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

@NotThreadSafe
public class ChronicleNetworkSynchronousReader extends AChronicleNetworkSynchronousChannel
        implements ISynchronousReader<IByteBuffer> {

    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;

    public ChronicleNetworkSynchronousReader(final ChronicleSocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        socket.shutdownOutput();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = buffer.asNioByteBuffer(0, socketSize);
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
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
            readFully(socketChannel, buffer.asNioByteBuffer(messageBuffer.position(), remaining));
            if (buffer.capacity() != capacityBefore) {
                messageBuffer = buffer.asNioByteBuffer(0, socketSize);
            }
        }

        ByteBuffers.position(messageBuffer, 0);
        if (ClosedByteBuffer.isClosed(buffer, MESSAGE_INDEX, size)) {
            close();
            throw new EOFException("closed by other side");
        }
        return buffer.slice(MESSAGE_INDEX, size);
    }

    public static void readFully(final ChronicleSocketChannel src, final java.nio.ByteBuffer byteBuffer)
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
