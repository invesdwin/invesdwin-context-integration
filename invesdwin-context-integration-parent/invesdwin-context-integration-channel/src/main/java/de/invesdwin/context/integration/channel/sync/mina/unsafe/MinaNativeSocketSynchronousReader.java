package de.invesdwin.context.integration.channel.sync.mina.unsafe;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.transport.socket.apr.AprSession;
import org.apache.mina.transport.socket.apr.AprSessionAccessor;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class MinaNativeSocketSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private MinaSocketSynchronousChannel channel;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private long fd;
    private int position = 0;
    private int bufferOffset = 0;
    private int messageTargetPosition = 0;

    public MinaNativeSocketSynchronousReader(final MinaSocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        if (!channel.getType().isNative()) {
            throw UnknownArgumentException.newInstance(IMinaSocketType.class, channel.getType());
        }
    }

    @Override
    public void open() throws IOException {
        channel.open(ch -> {
            //make sure Mina does not process any bytes
            ch.suspendWrite();
            ch.suspendRead();
            //validate connection
        }, true);
        final AprSession session = (AprSession) channel.getIoSession();
        fd = AprSessionAccessor.getDescriptor(session);
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = buffer.asNioByteBuffer(0, channel.getSocketSize());
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer = null;
            messageBuffer = null;
            fd = 0;
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
                throw FastEOFException.getInstance("non positive size");
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

    @SuppressWarnings("deprecation")
    public static int read0(final long src, final java.nio.ByteBuffer byteBuffer, final int position,
            final int remaining) throws IOException {
        int count = Socket.recvb(src, byteBuffer, position, remaining);
        if (count < 0) { // EOF
            if (Status.APR_STATUS_IS_EOF(-count)) {
                count = 0;
            } else if (Status.APR_STATUS_IS_EAGAIN(-count)) {
                count = 0;
            } else {
                throw MinaSocketSynchronousChannel.newTomcatException(count);
            }
        }
        return count;
    }

}
