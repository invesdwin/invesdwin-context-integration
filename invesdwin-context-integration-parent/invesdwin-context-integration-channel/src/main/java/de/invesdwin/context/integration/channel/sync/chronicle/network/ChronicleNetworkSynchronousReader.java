package de.invesdwin.context.integration.channel.sync.chronicle.network;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.InputStreams;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

@NotThreadSafe
public class ChronicleNetworkSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private ChronicleNetworkSynchronousChannel channel;
    private final int socketSize;
    private ChronicleSocketChannel socketChannel;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;

    public ChronicleNetworkSynchronousReader(final ChronicleNetworkSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        if (!channel.isWriterRegistered()) {
            channel.getSocket().shutdownOutput();
        }
        socketChannel = channel.getSocketChannel();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = buffer.asNioByteBuffer(0, socketSize);
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer = null;
            messageBuffer = null;
            socketChannel = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
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
    public IByteBufferProvider readMessage() throws IOException {
        int targetPosition = ChronicleNetworkSynchronousChannel.MESSAGE_INDEX;
        //read size
        int tries = 0;
        while (messageBuffer.position() < targetPosition) {
            socketChannel.read(messageBuffer);
            tries++;
            if (tries > InputStreams.MAX_READ_FULLY_TRIES) {
                close();
                throw FastEOFException.getInstance("read tries exceeded");
            }
        }
        final int size = buffer.getInt(ChronicleNetworkSynchronousChannel.SIZE_INDEX);
        if (size <= 0) {
            close();
            throw FastEOFException.getInstance("non positive size");
        }
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
        if (ClosedByteBuffer.isClosed(buffer, ChronicleNetworkSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return buffer.slice(ChronicleNetworkSynchronousChannel.MESSAGE_INDEX, size);
    }

    @Override
    public void readFinished() {
        //noop
    }

    public static void readFully(final ChronicleSocketChannel src, final java.nio.ByteBuffer byteBuffer)
            throws IOException {
        final int positionBefore = byteBuffer.position();
        int remaining = byteBuffer.remaining();
        int tries = 0;
        while (remaining > 0) {
            final int count = src.read(byteBuffer);
            if (count < 0) { // EOF
                break;
            }
            remaining -= count;
            tries++;
            if (tries > InputStreams.MAX_READ_FULLY_TRIES) {
                throw FastEOFException.getInstance("read tries exceeded");
            }
        }
        ByteBuffers.position(byteBuffer, positionBefore);
        if (remaining > 0) {
            throw ByteBuffers.newPutBytesToEOF();
        }
    }

}
