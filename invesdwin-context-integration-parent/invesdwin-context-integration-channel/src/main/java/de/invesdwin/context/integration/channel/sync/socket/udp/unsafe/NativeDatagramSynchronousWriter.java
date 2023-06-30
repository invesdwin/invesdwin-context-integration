package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ProtocolFamily;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
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

    private static final MethodHandle CHANNEL_SEND0_MH;
    private static final MethodHandle CHANNEL_TARGETSOCKADDR_GETTER_MH;
    private static final MethodHandle CHANNEL_FAMILY_GETTER_MH;
    private static final MethodHandle NATIVESOCKETADDRESS_ADDRESS_MH;
    private static final MethodHandle NATIVESOCKETADDRESS_ENCODE_MH;
    private static final MethodHandle FD_WRITE0_MH;

    static {
        try {
            final Class<?> dci = Class.forName("sun.nio.ch.DatagramChannelImpl");
            final Method send0 = Reflections.findMethod(dci, "send0", FileDescriptor.class, long.class, int.class,
                    long.class, int.class);
            Reflections.makeAccessible(send0);
            CHANNEL_SEND0_MH = MethodHandles.lookup().unreflect(send0);

            final Field targetSockAddr = Reflections.findField(dci, "targetSockAddr");
            Reflections.makeAccessible(targetSockAddr);
            CHANNEL_TARGETSOCKADDR_GETTER_MH = MethodHandles.lookup().unreflectGetter(targetSockAddr);

            final Field family = Reflections.findField(dci, "family");
            Reflections.makeAccessible(family);
            CHANNEL_FAMILY_GETTER_MH = MethodHandles.lookup().unreflectGetter(family);

            final Class<?> nsa = Class.forName("sun.nio.ch.NativeSocketAddress");
            final Method address = Reflections.findMethod(nsa, "address");
            Reflections.makeAccessible(address);
            NATIVESOCKETADDRESS_ADDRESS_MH = MethodHandles.lookup().unreflect(address);

            final Method encode = Reflections.findMethod(nsa, "encode", ProtocolFamily.class, InetSocketAddress.class);
            Reflections.makeAccessible(encode);
            NATIVESOCKETADDRESS_ENCODE_MH = MethodHandles.lookup().unreflect(encode);

            final Class<?> fdi = Class.forName("sun.nio.ch.DatagramDispatcher");
            final Method write0 = Reflections.findMethod(fdi, "write0", FileDescriptor.class, long.class, int.class);
            if (write0 != null) {
                Reflections.makeAccessible(write0);
                FD_WRITE0_MH = MethodHandles.lookup().unreflect(write0);
            } else {
                final Method write0Fallback = Reflections.findMethod(net.openhft.chronicle.core.OS.class, "write0",
                        FileDescriptor.class, long.class, int.class);
                FD_WRITE0_MH = MethodHandles.lookup().unreflect(write0Fallback);
            }

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
    private boolean initialized;

    public NativeDatagramSynchronousWriter(final DatagramSynchronousChannel channel) {
        this.channel = channel;
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
        if (!channel.isServer()) {
            initialized = true;
        }
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
        initialized = false;
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
        final int count;
        if (!initialized) {
            count = send0(messageToWrite, position, remaining);
            initialized = true;
        } else {
            count = write0(fd, messageToWrite, position, remaining);
        }
        remaining -= count;
        position += count;
        return remaining > 0;
    }

    private int send0(final long address, final int position, final int length) throws IOException {
        final int res;
        try {
            System.out.println(
                    "TODO: configure target address and use correct method args, check if write0 afterwards correctly uses the configured address always or if we always have to use send0");
            res = (int) CHANNEL_SEND0_MH.invokeExact(channel.getSocketChannel(), fd, address + position, length);
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

    public static int write0(final FileDescriptor dst, final long address, final int position, final int length)
            throws IOException {
        final int res;
        try {
            res = (int) FD_WRITE0_MH.invokeExact(dst, address + position, length);
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
