package de.invesdwin.context.integration.channel.sync.mina.apr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class TomcatNativeDatagramSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private static final boolean SERVER = true;

    private TomcatNativeDatagramSynchronousChannel channel;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private long fd;
    private int position = 0;

    public TomcatNativeDatagramSynchronousReader(final InetSocketAddress socketAddress,
            final int estimatedMaxMessageSize) {
        this.channel = new TomcatNativeDatagramSynchronousChannel(socketAddress, SERVER, estimatedMaxMessageSize);
        this.channel.setReaderRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        fd = channel.getFileDescriptor();
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
            throw TomcatNativeDatagramSynchronousChannel.newTomcatException(count);
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
                        throw TomcatNativeDatagramSynchronousChannel.newTomcatException(count);
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
                    throw TomcatNativeDatagramSynchronousChannel.newTomcatException(count);
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
