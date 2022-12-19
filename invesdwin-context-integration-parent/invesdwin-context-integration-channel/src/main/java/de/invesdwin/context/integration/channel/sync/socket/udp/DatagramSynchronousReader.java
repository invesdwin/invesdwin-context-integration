package de.invesdwin.context.integration.channel.sync.socket.udp;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.InputStreams;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class DatagramSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    public static final boolean SERVER = true;
    private DatagramSynchronousChannel channel;
    private final int socketSize;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private int bufferOffset = 0;
    private DatagramChannel socketChannel;

    public DatagramSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this(new DatagramSynchronousChannel(socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public DatagramSynchronousReader(final DatagramSynchronousChannel channel) {
        this.channel = channel;
        if (channel.isServer() != SERVER) {
            throw new IllegalStateException("datagram reader has to be the server");
        }
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = buffer.asNioByteBuffer(0, socketSize);
        socketChannel = channel.getSocketChannel();
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        messageBuffer = null;
        socketChannel = null;
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
        final SocketAddress client = socketChannel.receive(messageBuffer);
        return client != null;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        int targetPosition = bufferOffset + DatagramSynchronousChannel.MESSAGE_INDEX;
        //read size
        int tries = 0;
        while (messageBuffer.position() < targetPosition) {
            socketChannel.receive(messageBuffer);
            tries++;
            if (tries > InputStreams.MAX_READ_FULLY_TRIES) {
                close();
                throw FastEOFException.getInstance("read tries exceeded");
            }
        }
        final int size = buffer.getInt(bufferOffset + DatagramSynchronousChannel.SIZE_INDEX);
        if (size <= 0) {
            close();
            throw FastEOFException.getInstance("non positive size");
        }
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - messageBuffer.position();
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            buffer.ensureCapacity(targetPosition);
            if (buffer.capacity() != capacityBefore) {
                final int positionBefore = messageBuffer.position();
                messageBuffer = buffer.asNioByteBuffer(0, buffer.capacity());
                ByteBuffers.position(messageBuffer, positionBefore);
            }
            tries = 0;
            while (messageBuffer.position() < targetPosition) {
                socketChannel.receive(messageBuffer);
                tries++;
                if (tries > InputStreams.MAX_READ_FULLY_TRIES) {
                    close();
                    throw FastEOFException.getInstance("read tries exceeded");
                }
            }
        }

        final int offset = DatagramSynchronousChannel.MESSAGE_INDEX + size;
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + DatagramSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        final IByteBuffer message = buffer.slice(bufferOffset + DatagramSynchronousChannel.MESSAGE_INDEX, size);
        if (messageBuffer.position() > (bufferOffset + offset)) {
            /*
             * can be a maximum of a few messages we read like this because of the size in hasNext, the next read in
             * hasNext will be done with position 0
             */
            bufferOffset += offset;
        } else {
            bufferOffset = 0;
            ByteBuffers.position(messageBuffer, 0);
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

}
