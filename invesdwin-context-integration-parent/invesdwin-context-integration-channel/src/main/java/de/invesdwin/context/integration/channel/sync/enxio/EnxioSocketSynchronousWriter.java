package de.invesdwin.context.integration.channel.sync.enxio;

import java.io.FileDescriptor;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.time.duration.Duration;
import jnr.enxio.channels.Native;
import jnr.enxio.channels.NativeAccessor;
import net.openhft.chronicle.core.Jvm;

@NotThreadSafe
public class EnxioSocketSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private SocketSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private FileDescriptor fd;
    private int fdVal;

    public EnxioSocketSynchronousWriter(final SocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        if (!channel.isReaderRegistered()) {
            if (channel.getSocket() != null) {
                channel.getSocket().shutdownInput();
            }
        }
        fd = Jvm.getValue(channel.getSocketChannel(), "fd");
        fdVal = Jvm.getValue(fd, "fd");
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, SocketSynchronousChannel.MESSAGE_INDEX);
    }

    @Override
    public void close() {
        if (buffer != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            fd = null;
            fdVal = 0;
            buffer = null;
            messageBuffer = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        try {
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(SocketSynchronousChannel.SIZE_INDEX, size);
            writeFully(fdVal, buffer.asNioByteBuffer(), 0, SocketSynchronousChannel.MESSAGE_INDEX + size);
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public boolean writeFinished() throws IOException {
        return true;
    }

    public static void writeFully(final int dst, final java.nio.ByteBuffer buffer, final int pos, final int length)
            throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int position = pos;
        int remaining = length - pos;
        while (remaining > 0) {
            final int count = write0(dst, buffer, position, remaining);
            if (count < 0) { // EOF
                break;
            }
            if (count == 0 && timeout != null) {
                if (zeroCountNanos == -1) {
                    zeroCountNanos = System.nanoTime();
                } else if (timeout.isLessThanNanos(System.nanoTime() - zeroCountNanos)) {
                    throw FastEOFException.getInstance("write timeout exceeded");
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

    public static int write0(final int dst, final java.nio.ByteBuffer buffer, final int position, final int length)
            throws IOException {
        final int positionBefore = buffer.position();
        try {
            ByteBuffers.position(buffer, position);
            final int res = NativeAccessor.libc().write(dst, buffer, length);
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
