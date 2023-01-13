package de.invesdwin.context.integration.channel.sync.socket.sctp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.sctp.SctpSynchronousChannel;
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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;

@NotThreadSafe
public class NativeSctpSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private static final MethodHandle SCTPCHANNELIMPL_SEND0_METHOD;

    static {
        try {
            //static native int send0(int fd, long address, int length, InetAddress addr, int port, int assocId, int streamNumber, boolean unordered, int ppid) throws IOException;
            final Class<?> fdi = Class.forName("sun.nio.ch.sctp.SctpChannelImpl");
            final Method send0 = Jvm.getMethod(fdi, "send0", int.class, long.class, int.class, InetAddress.class,
                    int.class, int.class, int.class, boolean.class, int.class);
            SCTPCHANNELIMPL_SEND0_METHOD = MethodHandles.lookup().unreflect(send0);
        } catch (final ClassNotFoundException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private SctpSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private FileDescriptor fd;
    private int fdVal;

    public NativeSctpSynchronousWriter(final SctpSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
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
            writeFully(fdVal, buffer.addressOffset(), 0, SocketSynchronousChannel.MESSAGE_INDEX + size);
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    public static void writeFully(final int dst, final long address, final int pos, final int length)
            throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int position = pos;
        int remaining = length - pos;
        while (remaining > 0) {
            final int count = send0(dst, address + position, remaining, null, 0, -1, 0, false, 0);
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

    public static int send0(final int fd, final long address, final int length, final InetAddress addr, final int port,
            final int assocId, final int streamNumber, final boolean unordered, final int ppid) throws IOException {
        try {
            final int res = (int) SCTPCHANNELIMPL_SEND0_METHOD.invokeExact(fd, address, length, addr, port, assocId,
                    streamNumber, unordered, ppid);
            if (res == IOTools.IOSTATUS_INTERRUPTED) {
                return 0;
            } else {
                return IOTools.normaliseIOStatus(res);
            }
        } catch (final IOException ioe) {
            throw ioe;
        } catch (final Throwable e) {
            throw new IOException(e);
        }
    }

}
