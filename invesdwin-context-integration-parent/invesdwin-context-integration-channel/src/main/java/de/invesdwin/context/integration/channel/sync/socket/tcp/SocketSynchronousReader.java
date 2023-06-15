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

    protected SocketSynchronousChannel channel;
    protected final int socketSize;
    protected IByteBuffer buffer;
    protected java.nio.ByteBuffer messageBuffer;
    protected SocketChannel socketChannel;
    protected int position = 0;
    protected int bufferOffset = 0;
    protected int messageTargetPosition = 0;

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
        if (buffer != null) {
            buffer = null;
            messageBuffer = null;
            socketChannel = null;
            position = 0;
            bufferOffset = 0;
            messageTargetPosition = 0;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (buffer == null) {
            throw FastEOFException.getInstance("socket closed");
        }
        return hasMessage();
    }

    private boolean hasMessage() throws IOException {
        if (messageTargetPosition == 0) {
            final int sizeTargetPosition = bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX;
            //allow reading further than required to reduce the syscalls if possible
            if (!readFurther(sizeTargetPosition, buffer.remaining(position))) {
                return false;
            }
            final int size = buffer.getInt(bufferOffset + SocketSynchronousChannel.SIZE_INDEX);
            if (size <= 0) {
                close();
                throw FastEOFException.getInstance("non positive size: %s", size);
            }
            this.messageTargetPosition = sizeTargetPosition + size;
            if (buffer.capacity() < messageTargetPosition) {
                buffer.ensureCapacity(messageTargetPosition);
                messageBuffer = buffer.asNioByteBuffer();
            }
        }
        /*
         * only read as much further as required, so that we have a message where we can reset the position to 0 so the
         * expandable buffer does not grow endlessly due to fragmented messages piling up at the end each time.
         */
        return readFurther(messageTargetPosition, messageTargetPosition - position);
    }

    protected boolean readFurther(final int targetPosition, final int readLength) throws IOException {
        if (position < targetPosition) {
            position += read0(socketChannel, messageBuffer, position, readLength);
        }
        return position >= targetPosition;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int size = messageTargetPosition - bufferOffset - SocketSynchronousChannel.MESSAGE_INDEX;
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }

        final IByteBuffer message = buffer.slice(bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX, size);
        final int offset = SocketSynchronousChannel.MESSAGE_INDEX + size;
        if (position > (bufferOffset + offset)) {
            /*
             * can be a maximum of a few messages we read like this because of the size in hasNext, the next read in
             * hasNext will be done with position 0
             */
            bufferOffset += offset;
        } else {
            bufferOffset = 0;
            position = 0;
        }
        messageTargetPosition = 0;
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

    public static int read0(final SocketChannel channel, final java.nio.ByteBuffer buffer, final int position,
            final int length) throws IOException, FastEOFException {
        ByteBuffers.position(buffer, position);
        ByteBuffers.limit(buffer, position + length);
        try {
            final int count = channel.read(buffer);
            if (count < 0) { // EOF
                throw ByteBuffers.newEOF();
            }
            return count;
        } finally {
            buffer.clear();
        }
    }

}
