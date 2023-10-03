package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ProtocolFamily;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.FileChannelImplAccessor;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.context.integration.network.IOStatusAccessor;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

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
                final Method write0Fallback = Reflections.findMethod(FileChannelImplAccessor.class, "write0",
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
    private SocketAddress recipient;
    private int targetAddressLen;
    private long targetAddress;

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
        fd = Reflections.getBeanPathValue(channel.getSocketChannel(), "fd");
    }

    @Override
    public void close() throws IOException {
        final DatagramSynchronousChannel channelCopy = channel;
        if (fd != null) {
            if (channelCopy != null) {
                if (!channelCopy.isServer() || !channelCopy.isMultipleClientsAllowed() && recipient != null) {
                    try {
                        writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
                    } catch (final Throwable t) {
                        //ignore
                    }
                }
            }
            buffer = null;
            messageBuffer = null;
            fd = null;
            messageToWrite = 0;
            position = 0;
            remaining = 0;
            recipient = null;
            targetAddress = 0;
            targetAddressLen = 0;
        }
        if (channelCopy != null) {
            channelCopy.close();
            channel = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        if (channel.isServer()) {
            maybeInitTargetAddress();
        }
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

    private void maybeInitTargetAddress() {
        if (recipient == null
                || channel.isMultipleClientsAllowed() && !recipient.equals(channel.getOtherSocketAddress())) {
            try {
                recipient = channel.getOtherSocketAddress();
                final Object targetSockAddr = CHANNEL_TARGETSOCKADDR_GETTER_MH.invoke(channel.getSocketChannel());
                final ProtocolFamily family = (ProtocolFamily) CHANNEL_FAMILY_GETTER_MH
                        .invoke(channel.getSocketChannel());
                this.targetAddressLen = (int) NATIVESOCKETADDRESS_ENCODE_MH.invoke(targetSockAddr, family, recipient);
                this.targetAddress = (long) NATIVESOCKETADDRESS_ADDRESS_MH.invoke(targetSockAddr);
            } catch (final Throwable e) {
                throw new RuntimeException(e);
            }
        }
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
        if (channel.isServer()) {
            count = send0(messageToWrite, position, remaining);
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
            res = (int) CHANNEL_SEND0_MH.invokeExact(fd, address + position, length, targetAddress, targetAddressLen);
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
        if (res == IOStatusAccessor.INTERRUPTED) {
            return 0;
        } else {
            final int count = IOStatusAccessor.normalize(res);
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
        if (res == IOStatusAccessor.INTERRUPTED) {
            return 0;
        } else {
            final int count = IOStatusAccessor.normalize(res);
            if (count < 0) { // EOF
                throw ByteBuffers.newEOF();
            }
            return count;
        }
    }

}
