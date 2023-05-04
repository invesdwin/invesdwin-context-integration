package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;

@NotThreadSafe
public class NativeDatagramSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    public static final boolean SERVER = false;

    private static final MethodHandle WRITE0_MH;

    static {
        try {
            final Class<?> fdi = Class.forName("sun.nio.ch.DatagramDispatcher");
            final Method write0 = Reflections.findMethod(fdi, "write0", FileDescriptor.class, long.class, int.class);
            Reflections.makeAccessible(write0);
            WRITE0_MH = MethodHandles.lookup().unreflect(write0);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DatagramSynchronousChannel channel;
    private IByteBuffer buffer;
    private IByteBuffer messageBuffer;
    private FileDescriptor fd;
    private final int socketSize;
    private long messageToWrite;
    private int position;
    private int remaining;

    public NativeDatagramSynchronousWriter(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this(new DatagramSynchronousChannel(socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public NativeDatagramSynchronousWriter(final DatagramSynchronousChannel channel) {
        this.channel = channel;
        if (channel.isServer() != SERVER) {
            throw new IllegalStateException("datagram writer has to be the client");
        }
        this.channel.setWriterRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, DatagramSynchronousChannel.MESSAGE_INDEX);
        fd = Jvm.getValue(channel.getSocketChannel(), "fd");
    }

    @Override
    public void close() throws IOException {
        if (fd != null) {
            try {
                writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
            messageBuffer = null;
            fd = null;
            messageToWrite = 0;
            position = 0;
            remaining = 0;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int size = message.getBuffer(messageBuffer);
        final int datagramSize = DatagramSynchronousChannel.MESSAGE_INDEX + size;
        if (datagramSize > socketSize) {
            throw new IllegalArgumentException(
                    "Data truncation would occur: datagramSize[" + datagramSize + "] > socketSize[" + socketSize + "]");
        }
        buffer.putInt(DatagramSynchronousChannel.SIZE_INDEX, size);
        messageToWrite = buffer.addressOffset();
        position = 0;
        remaining = datagramSize;
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (messageToWrite == 0) {
            return true;
        } else if (!writeFurther()) {
            messageToWrite = 0;
            position = 0;
            remaining = 0;
            return true;
        } else {
            return false;
        }
    }

    private boolean writeFurther() throws IOException {
        final int count = NativeSocketSynchronousWriter.write0(fd, messageToWrite, position, remaining);
        remaining -= count;
        position += count;
        return remaining > 0;
    }

    public static int write0(final FileDescriptor dst, final long address, final int position, final int length)
            throws IOException {
        final int res;
        try {
            res = (int) WRITE0_MH.invokeExact(dst, address + position, length);
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
        if (res == IOTools.IOSTATUS_INTERRUPTED) {
            return 0;
        } else {
            final int count = IOTools.normaliseIOStatus(res);
            if (count < 0) { // EOF
                throw ByteBuffers.newEOF();
            }
            return count;
        }
    }

}
