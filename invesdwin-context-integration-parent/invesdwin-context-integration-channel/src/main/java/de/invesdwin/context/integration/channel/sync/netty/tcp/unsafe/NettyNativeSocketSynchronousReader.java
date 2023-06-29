package de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.UnixChannel;

@NotThreadSafe
public class NettyNativeSocketSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private NettySocketSynchronousChannel channel;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private FileDescriptor fd;
    private int position = 0;
    private int bufferOffset = 0;
    private int messageTargetPosition = 0;

    public NettyNativeSocketSynchronousReader(final NettySocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open(ch -> {
            //make sure netty does not process any bytes
            if (!channel.isWriterRegistered()) {
                ch.shutdownOutput();
            }
        });
        channel.getSocketChannel().deregister();
        final UnixChannel unixChannel = (UnixChannel) channel.getSocketChannel();
        channel.closeBootstrapAsync();
        fd = unixChannel.fd();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = buffer.asNioByteBuffer(0, channel.getSocketSize());
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer = null;
            messageBuffer = null;
            fd = null;
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
            final int sizeTargetPosition = bufferOffset + NettySocketSynchronousChannel.MESSAGE_INDEX;
            //allow reading further than required to reduce the syscalls if possible
            if (!readFurther(sizeTargetPosition, buffer.remaining(position))) {
                return false;
            }
            final int size = buffer.getInt(bufferOffset + NettySocketSynchronousChannel.SIZE_INDEX);
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

    private boolean readFurther(final int targetPosition, final int readLength) throws IOException {
        if (position < targetPosition) {
            position += read0(fd, messageBuffer, position, readLength);
        }
        return position >= targetPosition;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int size = messageTargetPosition - bufferOffset - NettySocketSynchronousChannel.MESSAGE_INDEX;
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + NettySocketSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }

        final IByteBuffer message = buffer.slice(bufferOffset + NettySocketSynchronousChannel.MESSAGE_INDEX, size);
        final int offset = NettySocketSynchronousChannel.MESSAGE_INDEX + size;
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

    public static int read0(final FileDescriptor fd, final java.nio.ByteBuffer buffer, final int position,
            final int length) throws IOException {
        final int count = fd.read(buffer, position, length);
        //netty checks return code itself and converts 0 count to -1 sadly
        if (count > 0) {
            return count;
        } else {
            return 0;
        }
    }

}
