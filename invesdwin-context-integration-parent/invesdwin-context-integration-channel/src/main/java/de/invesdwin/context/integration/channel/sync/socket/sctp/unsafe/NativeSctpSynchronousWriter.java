package de.invesdwin.context.integration.channel.sync.socket.sctp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.IOStatusAccessor;
import de.invesdwin.context.integration.channel.sync.socket.sctp.SctpSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class NativeSctpSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private static final MethodHandle SCTPCHANNELIMPL_SEND0_METHOD;

    static {
        try {
            //only OracleJDK contains SCTP as it seems (causes compile errors in maven/jenkins): https://stackoverflow.com/a/26614215
            //static native int send0(int fd, long address, int length, InetAddress addr, int port, int assocId, int streamNumber, boolean unordered, int ppid) throws IOException;
            final Class<?> sctpChannelImplClass = Class.forName("sun.nio.ch.sctp.SctpChannelImpl");
            final Method send0Method = Reflections.findMethod(sctpChannelImplClass, "send0", int.class, long.class,
                    int.class, InetAddress.class, int.class, int.class, int.class, boolean.class, int.class);
            Reflections.makeAccessible(send0Method);
            SCTPCHANNELIMPL_SEND0_METHOD = MethodHandles.lookup().unreflect(send0Method);
        } catch (final ClassNotFoundException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private SctpSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private FileDescriptor fd;
    private int fdVal;
    private long messageToWrite;
    private int position;
    private int remaining;

    public NativeSctpSynchronousWriter(final SctpSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        fd = Reflections.getBeanPathValue(channel.getSocketChannel(), "fd");
        fdVal = Reflections.getBeanPathValue(fd, "fd");
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, SocketSynchronousChannel.MESSAGE_INDEX);
    }

    @Override
    public void close() {
        if (buffer != null) {
            try {
                writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            fd = null;
            fdVal = 0;
            buffer = null;
            messageBuffer = null;
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
        try {
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(SocketSynchronousChannel.SIZE_INDEX, size);
            messageToWrite = buffer.addressOffset();
            position = 0;
            remaining = SctpSynchronousChannel.MESSAGE_INDEX + size;
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
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
        final int count = write0(fdVal, messageToWrite, position, remaining);
        remaining -= count;
        position += count;
        return remaining > 0;
    }

    private static int write0(final int dst, final long address, final int position, final int remaining)
            throws IOException, FastEOFException {
        final int count = send0(dst, address + position, remaining, null, 0, -1, 0, false, 0);
        if (count < 0) { // EOF
            throw ByteBuffers.newEOF();
        }
        return count;
    }

    private static int send0(final int fd, final long address, final int length, final InetAddress addr, final int port,
            final int assocId, final int streamNumber, final boolean unordered, final int ppid) throws IOException {
        try {
            final int res = (int) SCTPCHANNELIMPL_SEND0_METHOD.invokeExact(fd, address, length, addr, port, assocId,
                    streamNumber, unordered, ppid);
            if (res == IOStatusAccessor.INTERRUPTED) {
                return 0;
            } else {
                return IOStatusAccessor.normalize(res);
            }
        } catch (final IOException ioe) {
            throw ioe;
        } catch (final Throwable e) {
            throw new IOException(e);
        }
    }

}
