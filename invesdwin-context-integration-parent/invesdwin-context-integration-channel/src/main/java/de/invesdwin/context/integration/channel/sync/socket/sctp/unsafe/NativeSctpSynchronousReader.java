package de.invesdwin.context.integration.channel.sync.socket.sctp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.sctp.SctpSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;

@NotThreadSafe
public class NativeSctpSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private static final MethodHandle SCTPCHANNELIMPL_RECEIVE0_METHOD;
    private static final MethodHandle RESULTCONTAINER_CLEAR_METHOD;

    static {
        try {
            //static native int receive0(int fd, ResultContainer resultContainer, long address, int length, boolean peek) throws IOException;
            final Class<?> sctpChannelImpl = Class.forName("sun.nio.ch.sctp.SctpChannelImpl");
            final Method receive0 = Jvm.getMethod(sctpChannelImpl, "receive0", int.class,
                    sun.nio.ch.sctp.ResultContainer.class, long.class, int.class, boolean.class);
            SCTPCHANNELIMPL_RECEIVE0_METHOD = MethodHandles.lookup().unreflect(receive0);

            final Class<?> resultContainer = Class.forName("sun.nio.ch.sctp.ResultContainer");
            final Method clear = Jvm.getMethod(resultContainer, "clear");
            RESULTCONTAINER_CLEAR_METHOD = MethodHandles.lookup().unreflect(clear);
        } catch (final ClassNotFoundException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private SctpSynchronousChannel channel;
    private final int socketSize;
    private IByteBuffer buffer;
    private FileDescriptor fd;
    private int position = 0;
    private int bufferOffset = 0;
    private int fdVal;
    private sun.nio.ch.sctp.ResultContainer resultContainer;

    public NativeSctpSynchronousReader(final SctpSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        fd = Jvm.getValue(channel.getSocketChannel(), "fd");
        fdVal = Jvm.getValue(fd, "fd");
        resultContainer = new sun.nio.ch.sctp.ResultContainer();
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
            resultContainer = null;
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
        final int count = receive(fdVal, buffer.addressOffset(), position, buffer.remaining(position));
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
            final int count = receive(fdVal, buffer.addressOffset(), position, targetPosition - position);
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
                final int count = receive(fdVal, buffer.addressOffset(), position, remaining);
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

    private int receive(final int fd, final long address, final int position, final int length) throws IOException {
        return receive0(fd, resultContainer, address + position, length, false);
    }

    public static int receive0(final int fd, final sun.nio.ch.sctp.ResultContainer resultContainer, final long address,
            final int length, final boolean peek) throws IOException {
        try {
            RESULTCONTAINER_CLEAR_METHOD.invoke(resultContainer);
            final int res = (int) SCTPCHANNELIMPL_RECEIVE0_METHOD.invokeExact(fd, resultContainer, address, length,
                    peek);
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