package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class SocketSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private SocketSynchronousChannel channel;
    private final int socketSize;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private int bufferOffset = 0;
    private SocketChannel socketChannel;

    public SocketSynchronousReader(final SocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //remove volatile access
        if (!channel.isWriterRegistered()) {
            if (channel.getSocket() != null) {
                channel.getSocket().shutdownOutput();
            }
        }
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
        final int read = socketChannel.read(messageBuffer);
        return read > 0;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        int targetPosition = SocketSynchronousChannel.MESSAGE_INDEX;
        int size = 0;
        //read size
        while ((messageBuffer.position() - bufferOffset) < targetPosition) {
            socketChannel.read(messageBuffer);
        }
        size = buffer.getInt(bufferOffset + SocketSynchronousChannel.SIZE_INDEX);
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - messageBuffer.position();
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            buffer.putBytesTo(bufferOffset + messageBuffer.position(), socketChannel, remaining);
            if (buffer.capacity() != capacityBefore) {
                messageBuffer = buffer.asNioByteBuffer(0, socketSize);
            }
        }

        final int offset = SocketSynchronousChannel.MESSAGE_INDEX + size;
        ByteBuffers.position(messageBuffer, messageBuffer.position() - offset);
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        final IByteBuffer message = buffer.slice(bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX, size);
        if (messageBuffer.position() > 0) {
            //can be a maximum of 2 messages we read like this
            bufferOffset += offset;
        } else {
            bufferOffset = 0;
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

}
