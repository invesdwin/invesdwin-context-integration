package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;

@NotThreadSafe
public class NativeDatagramSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    public static final boolean SERVER = true;

    private static final MethodHandle READ0_MH;

    static {
        try {
            final Class<?> fdi = Class.forName("sun.nio.ch.DatagramDispatcher");
            final Method read0 = Reflections.findMethod(fdi, "read0", FileDescriptor.class, long.class, int.class);
            Reflections.makeAccessible(read0);
            READ0_MH = MethodHandles.lookup().unreflect(read0);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DatagramSynchronousChannel channel;
    private final int socketSize;
    private IByteBuffer buffer;
    private FileDescriptor fd;
    private int position;
    private final int truncatedSize;

    public NativeDatagramSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this(new DatagramSynchronousChannel(socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public NativeDatagramSynchronousReader(final DatagramSynchronousChannel channel) {
        this.channel = channel;
        if (channel.isServer() != SERVER) {
            throw new IllegalStateException("datagram reader has to be the server");
        }
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
        this.truncatedSize = socketSize - DatagramSynchronousChannel.MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        fd = Jvm.getValue(channel.getSocketChannel(), "fd");
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        fd = null;
        position = 0;
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
        if (position > 0) {
            return true;
        }
        final int count = read0(fd, buffer.addressOffset(), position, buffer.remaining(position));
        if (count < 0) {
            throw FastEOFException.getInstance("socket closed");
        }
        position += count;
        return count > 0;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        if (position > socketSize) {
            close();
            throw new IllegalArgumentException("data truncation occurred: position");
        }

        final int size = buffer.getInt(DatagramSynchronousChannel.SIZE_INDEX);
        if (size <= 0) {
            close();
            throw FastEOFException.getInstance("non positive size");
        }

        if (size > truncatedSize) {
            close();
            throw new IllegalArgumentException("data truncation occurred: size");
        }

        if (ClosedByteBuffer.isClosed(buffer, DatagramSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }

        final IByteBuffer message = buffer.slice(DatagramSynchronousChannel.MESSAGE_INDEX, size);
        position = 0;
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

    public static int read0(final FileDescriptor src, final long address, final int position, final int length)
            throws IOException {
        final int res;
        try {
            res = (int) READ0_MH.invokeExact(src, address + position, length);
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
        if (res == IOTools.IOSTATUS_INTERRUPTED) {
            return 0;
        } else {
            final int count = IOTools.normaliseIOStatus(res);
            if (count < 0) {
                throw FastEOFException.getInstance("socket closed");
            }
            return count;
        }
    }

}
