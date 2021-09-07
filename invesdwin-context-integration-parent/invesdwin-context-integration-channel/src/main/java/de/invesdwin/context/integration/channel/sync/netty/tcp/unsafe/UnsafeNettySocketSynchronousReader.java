package de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.UnixChannel;

/**
 * Since netty reads in an asynchronous handler thread and the bytebuffer can/should not be shared with other threads,
 * the ISerde has to either copy the buffer or better directly convert it to the appropiate value type (for zero copy).
 */
@NotThreadSafe
public class UnsafeNettySocketSynchronousReader extends NettySocketChannel implements ISynchronousReader<IByteBuffer> {

    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private FileDescriptor fd;
    private int position = 0;

    public UnsafeNettySocketSynchronousReader(final INettySocketChannelType type, final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open(channel -> {
            channel.shutdownOutput();
        });
        socketChannel.deregister();
        final UnixChannel unixChannel = (UnixChannel) socketChannel;
        fd = unixChannel.fd();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = buffer.asByteBuffer(0, socketSize);
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer = null;
            messageBuffer = null;
            fd = null;
        }
        super.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (position > 0) {
            return true;
        }
        final int read = fd.read(messageBuffer, 0, socketSize);
        if (read > 0) {
            position = read;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        int targetPosition = MESSAGE_INDEX;
        int size = 0;
        //read size
        while (position < targetPosition) {
            final int read = fd.read(messageBuffer, 0, socketSize);
            position += read;
        }
        size = buffer.getInt(SIZE_INDEX);
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - position;
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            buffer.ensureCapacity(targetPosition);
            if (buffer.capacity() != capacityBefore) {
                messageBuffer = buffer.asByteBuffer(0, socketSize);
            }
            readFully(fd, messageBuffer, position, remaining);
        }
        position = 0;

        if (ClosedByteBuffer.isClosed(buffer, MESSAGE_INDEX, size)) {
            close();
            throw new EOFException("closed by other side");
        }
        return buffer.slice(MESSAGE_INDEX, size);
    }

    public static void readFully(final FileDescriptor src, final java.nio.ByteBuffer byteBuffer, final int pos,
            final int length) throws IOException {
        int position = pos;
        int remaining = length - pos;
        while (remaining > 0) {
            final int count = src.read(byteBuffer, position, remaining);
            if (count == -1) { // EOF
                break;
            }
            position += count;
            remaining -= count;
        }
        if (remaining > 0) {
            throw ByteBuffers.newPutBytesToEOF();
        }
    }

}
