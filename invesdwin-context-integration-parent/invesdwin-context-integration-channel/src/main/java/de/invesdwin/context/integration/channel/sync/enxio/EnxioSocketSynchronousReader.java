package de.invesdwin.context.integration.channel.sync.enxio;

import java.io.FileDescriptor;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;
import jnr.enxio.channels.Native;
import jnr.enxio.channels.NativeAccessor;
import net.openhft.chronicle.core.Jvm;

@NotThreadSafe
public class EnxioSocketSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private SocketSynchronousChannel channel;
    private final int socketSize;
    private IByteBuffer buffer;
    private FileDescriptor fd;
    private int fdVal;
    private int position = 0;
    private int bufferOffset = 0;

    public EnxioSocketSynchronousReader(final SocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        if (!channel.isWriterRegistered()) {
            if (channel.getSocket() != null) {
                channel.getSocket().shutdownOutput();
            }
        }
        fd = Jvm.getValue(channel.getSocketChannel(), "fd");
        fdVal = Jvm.getValue(fd, "fd");
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        position = 0;
    }

    @Override
    public void close() {
        if (buffer != null) {
            buffer = null;
            fd = null;
            fdVal = 0;
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
        final int count = read0(fdVal, buffer.asNioByteBuffer(), position, buffer.remaining(position));
        if (count < 0) {
            throw FastEOFException.getInstance("socket closed");
        }
        position += count;
        return count > 0;
    }

    //CHECKSTYLE:OFF
    @Override
    public IByteBufferProvider readMessage() throws IOException {
        //CHECKSTYLE:ON
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int targetPosition = bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX;
        //read size
        while (position < targetPosition) {
            final int count = read0(fdVal, buffer.asNioByteBuffer(), position, targetPosition - position);
            if (count < 0) { // EOF
                close();
                throw ByteBuffers.newEOF();
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
        final int size = buffer.getInt(bufferOffset + SocketSynchronousChannel.SIZE_INDEX);
        if (size <= 0) {
            close();
            throw FastEOFException.getInstance("non positive size");
        }
        targetPosition += size;
        //read message if not complete yet
        int remaining = targetPosition - position;
        if (remaining > 0) {
            buffer.ensureCapacity(targetPosition);
            while (position < targetPosition) {
                final int count = read0(fdVal, buffer.asNioByteBuffer(), position, remaining);
                if (count < 0) { // EOF
                    close();
                    throw ByteBuffers.newEOF();
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
                    remaining -= count;
                    position += count;
                }
            }
        }

        final int offset = SocketSynchronousChannel.MESSAGE_INDEX + size;
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }

        final IByteBuffer message = buffer.slice(bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX, size);
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
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

    public static int read0(final int src, final java.nio.ByteBuffer buffer, final int position, final int length)
            throws IOException {
        final int positionBefore = buffer.position();
        try {
            ByteBuffers.position(buffer, position);
            final int res = NativeAccessor.libc().read(src, buffer, length);
            if (res < 0) {
                switch (Native.getLastError()) {
                case EAGAIN:
                case EWOULDBLOCK:
                    return 0;
                default:
                    throw new IOException(Native.getLastErrorString());
                }
            } else {
                return res;
            }
        } finally {
            ByteBuffers.position(buffer, positionBefore);
        }
    }

}
