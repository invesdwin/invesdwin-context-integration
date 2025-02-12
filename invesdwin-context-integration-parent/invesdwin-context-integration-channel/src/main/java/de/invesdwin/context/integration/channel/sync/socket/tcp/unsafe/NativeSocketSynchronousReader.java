package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.net.PortUnreachableException;
import java.net.SocketException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.FileChannelImplAccessor;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.IOStatusAccessor;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class NativeSocketSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private static final MethodHandle READ0_MH;

    static {
        try {
            final Class<?> fdi = Class.forName("sun.nio.ch.SocketDispatcher");
            final Method read0 = Reflections.findMethod(fdi, "read0", FileDescriptor.class, long.class, int.class);
            if (read0 != null) {
                Reflections.makeAccessible(read0);
                READ0_MH = MethodHandles.lookup().unreflect(read0);
            } else {
                final Method write0Fallback = Reflections.findMethod(FileChannelImplAccessor.class, "read0",
                        FileDescriptor.class, long.class, int.class);
                READ0_MH = MethodHandles.lookup().unreflect(write0Fallback);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SocketSynchronousChannel channel;
    private final int socketSize;
    private IByteBuffer buffer;
    private FileDescriptor fd;
    private int position = 0;
    private int bufferOffset = 0;
    private int messageTargetPosition = 0;

    public NativeSocketSynchronousReader(final SocketSynchronousChannel channel) {
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
        fd = Reflections.getBeanPathValue(channel.getSocketChannel(), "fd");
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
    }

    @Override
    public void close() {
        if (buffer != null) {
            buffer = null;
            fd = null;
            position = 0;
            bufferOffset = 0;
            messageTargetPosition = 0;
        }
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
        return hasMessage();
    }

    private boolean hasMessage() throws IOException {
        if (messageTargetPosition == 0) {
            final int sizeTargetPosition = bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX;
            //allow reading further than required to reduce the syscalls if possible
            if (!readFurther(sizeTargetPosition, buffer.remaining(position))) {
                return false;
            }
            final int size = buffer.getInt(bufferOffset + SocketSynchronousChannel.SIZE_INDEX);
            if (size <= 0) {
                close();
                throw FastEOFException.getInstance("non positive size: %s", size);
            }
            this.messageTargetPosition = sizeTargetPosition + size;
            buffer.ensureCapacity(messageTargetPosition);
        }
        /*
         * only read as much further as required, so that we have a message where we can reset the position to 0 so the
         * expandable buffer does not grow endlessly due to fragmented messages piling up at the end each time.
         */
        return readFurther(messageTargetPosition, messageTargetPosition - position);
    }

    private boolean readFurther(final int targetPosition, final int readLength) throws IOException {
        if (position < targetPosition) {
            position += read0(fd, buffer.addressOffset(), position, readLength);
        }
        return position >= targetPosition;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int size = messageTargetPosition - bufferOffset - SocketSynchronousChannel.MESSAGE_INDEX;
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }

        final IByteBuffer message = buffer.slice(bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX, size);
        final int offset = SocketSynchronousChannel.MESSAGE_INDEX + size;
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
        messageTargetPosition = 0;
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
        } catch (final PortUnreachableException e) {
            throw e;
        } catch (final SocketException e) {
            throw e;
        } catch (final IOException e) {
            //java.io.IOException: Ungültiger Dateideskriptor
            throw FastEOFException.getInstance(e);
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
