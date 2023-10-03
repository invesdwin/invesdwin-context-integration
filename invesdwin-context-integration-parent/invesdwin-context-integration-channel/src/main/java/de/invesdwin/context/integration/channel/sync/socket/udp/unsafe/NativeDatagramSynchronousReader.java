package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.FileChannelImplAccessor;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.context.integration.network.IOStatusAccessor;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class NativeDatagramSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private static final MethodHandle CHANNEL_RECEIVE0_MH;
    private static final MethodHandle CHANNEL_SOURCESOCKADDR_GETTER_MH;
    private static final MethodHandle NATIVESOCKETADDRESS_ADDRESS_MH;
    private static final MethodHandle NATIVESOCKETADDRESS_DECODE_MH;
    private static final MethodHandle FD_READ0_MH;

    static {
        try {
            final Class<?> dci = Class.forName("sun.nio.ch.DatagramChannelImpl");
            final Method receive0 = Reflections.findMethod(dci, "receive0", FileDescriptor.class, long.class, int.class,
                    long.class, boolean.class);
            Reflections.makeAccessible(receive0);
            CHANNEL_RECEIVE0_MH = MethodHandles.lookup().unreflect(receive0);

            final Field sourceSockAddr = Reflections.findField(dci, "sourceSockAddr");
            Reflections.makeAccessible(sourceSockAddr);
            CHANNEL_SOURCESOCKADDR_GETTER_MH = MethodHandles.lookup().unreflectGetter(sourceSockAddr);

            final Class<?> nsa = Class.forName("sun.nio.ch.NativeSocketAddress");
            final Method address = Reflections.findMethod(nsa, "address");
            Reflections.makeAccessible(address);
            NATIVESOCKETADDRESS_ADDRESS_MH = MethodHandles.lookup().unreflect(address);

            final Method decode = Reflections.findMethod(nsa, "decode");
            Reflections.makeAccessible(decode);
            NATIVESOCKETADDRESS_DECODE_MH = MethodHandles.lookup().unreflect(decode);

            final Class<?> fdi = Class.forName("sun.nio.ch.DatagramDispatcher");
            final Method read0 = Reflections.findMethod(fdi, "read0", FileDescriptor.class, long.class, int.class);
            if (read0 != null) {
                Reflections.makeAccessible(read0);
                FD_READ0_MH = MethodHandles.lookup().unreflect(read0);
            } else {
                final Method write0Fallback = Reflections.findMethod(FileChannelImplAccessor.class, "read0",
                        FileDescriptor.class, long.class, int.class);
                FD_READ0_MH = MethodHandles.lookup().unreflect(write0Fallback);
            }
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
    private Object sourceSockAddr;
    private long senderAddress;

    public NativeDatagramSynchronousReader(final DatagramSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
        this.truncatedSize = socketSize - DatagramSynchronousChannel.MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        fd = Reflections.getBeanPathValue(channel.getSocketChannel(), "fd");

        try {
            this.sourceSockAddr = CHANNEL_SOURCESOCKADDR_GETTER_MH.invoke(channel.getSocketChannel());
            this.senderAddress = (long) NATIVESOCKETADDRESS_ADDRESS_MH.invoke(sourceSockAddr);
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
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
        sourceSockAddr = null;
        senderAddress = 0;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (buffer == null) {
            throw FastEOFException.getInstance("socket closed");
        }
        if (position > 0) {
            return true;
        }
        final int count;
        if (channel.isMultipleClientsAllowed() || channel.getOtherSocketAddress() == null) {
            count = receive0(buffer.addressOffset(), position, buffer.remaining(position));
            if (count > 0) {
                try {
                    final InetSocketAddress addr = (InetSocketAddress) NATIVESOCKETADDRESS_DECODE_MH
                            .invoke(sourceSockAddr);
                    channel.setOtherSocketAddress(addr);
                } catch (final Throwable e) {
                    throw new RuntimeException(e);
                }
            }

        } else {
            count = read0(fd, buffer.addressOffset(), position, buffer.remaining(position));
        }
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

    private int receive0(final long address, final int position, final int length) throws IOException {
        final int res;
        try {
            res = (int) CHANNEL_RECEIVE0_MH.invokeExact(fd, address + position, length, senderAddress, true);
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
        if (res == IOStatusAccessor.INTERRUPTED) {
            return 0;
        } else {
            final int count = IOStatusAccessor.normalize(res);
            if (count < 0) {
                throw FastEOFException.getInstance("socket closed");
            }
            return count;
        }
    }

    public static int read0(final FileDescriptor src, final long address, final int position, final int length)
            throws IOException {
        final int res;
        try {
            res = (int) FD_READ0_MH.invokeExact(src, address + position, length);
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
        if (res == IOStatusAccessor.INTERRUPTED) {
            return 0;
        } else {
            final int count = IOStatusAccessor.normalize(res);
            if (count < 0) {
                throw FastEOFException.getInstance("socket closed");
            }
            return count;
        }
    }

}
