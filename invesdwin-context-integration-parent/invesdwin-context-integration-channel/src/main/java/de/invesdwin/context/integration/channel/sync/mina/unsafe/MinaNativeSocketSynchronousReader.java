package de.invesdwin.context.integration.channel.sync.mina.unsafe;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.transport.socket.apr.AprSession;
import org.apache.mina.transport.socket.apr.AprSessionAccessor;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.context.integration.channel.sync.mina.type.MinaSocketType;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class MinaNativeSocketSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private MinaSocketSynchronousChannel channel;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private long fd;
    private int position = 0;

    public MinaNativeSocketSynchronousReader(final MinaSocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        if (channel.getType() != MinaSocketType.AprTcp) {
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
            fd = -1;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (position > 0) {
            return true;
        }
        final int count = Socket.recvb(fd, messageBuffer, 0, channel.getSocketSize());
        if (count > 0) {
            position = count;
            return true;
        } else if (count < 0) {
            if (Status.APR_STATUS_IS_EAGAIN(-count)) {
                return false;
            }
            throw FastEOFException.getInstance("closed by other side");
        } else {
            return false;
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int targetPosition = MinaSocketSynchronousChannel.MESSAGE_INDEX;
        //read size
        try {
            while (position < targetPosition) {
                int count = Socket.recvb(fd, messageBuffer, 0, channel.getSocketSize());
                if (count < 0) { // EOF
                    if (Status.APR_STATUS_IS_EOF(-count)) {
                        count = 0;
                    } else if (Status.APR_STATUS_IS_EAGAIN(-count)) {
                        count = 0;
                    } else {
                        close();
                        throw MinaNativeSocketSynchronousWriter.newException(count);
                    }
                }
                if (count == 0 && timeout != null) {
                    if (zeroCountNanos == -1) {
                        zeroCountNanos = System.nanoTime();
                    } else if (timeout.isLessThanNanos(System.nanoTime() - zeroCountNanos)) {
                        close();
                        throw FastEOFException.getInstance("read timeout exceeded");
                    }
                    ASpinWait.onSpinWaitStatic();
                } else {
                    zeroCountNanos = -1L;
                    position += count;
                }
            }
        } catch (final ClosedChannelException e) {
            throw FastEOFException.getInstance(e);
        }
        final int size = buffer.getInt(MinaSocketSynchronousChannel.SIZE_INDEX);
        if (size <= 0) {
            close();
            throw FastEOFException.getInstance("non positive size");
        }
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - position;
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            buffer.ensureCapacity(targetPosition);
            if (buffer.capacity() != capacityBefore) {
                messageBuffer = buffer.asNioByteBuffer(0, channel.getSocketSize());
            }
            readFully(fd, messageBuffer, position, remaining);
        }
        position = 0;

        if (ClosedByteBuffer.isClosed(buffer, MinaSocketSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return buffer.slice(MinaSocketSynchronousChannel.MESSAGE_INDEX, size);
    }

    @Override
    public void readFinished() {
        //noop
    }

    public static void readFully(final long src, final java.nio.ByteBuffer byteBuffer, final int pos, final int length)
            throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int position = pos;
        int remaining = length - pos;
        while (remaining > 0) {
            int count = Socket.recvb(src, byteBuffer, position, remaining);
            if (count < 0) { // EOF
                if (Status.APR_STATUS_IS_EOF(-count)) {
                    count = 0;
                } else if (Status.APR_STATUS_IS_EAGAIN(-count)) {
                    count = 0;
                } else {
                    throw MinaNativeSocketSynchronousWriter.newException(count);
                }
            }
            if (count == 0 && timeout != null) {
                if (zeroCountNanos == -1) {
                    zeroCountNanos = System.nanoTime();
                } else if (timeout.isLessThanNanos(System.nanoTime() - zeroCountNanos)) {
                    throw FastEOFException.getInstance("read timeout exceeded");
                }
                ASpinWait.onSpinWaitStatic();
            } else {
                zeroCountNanos = -1L;
                position += count;
                remaining -= count;
            }
        }
        if (remaining > 0) {
            throw ByteBuffers.newEOF();
        }
    }

}
