package de.invesdwin.context.integration.channel.sync.socket.sctp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.net.SocketException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.IOStatusAccessor;
import de.invesdwin.context.integration.channel.sync.socket.sctp.SctpSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class NativeSctpSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private static final Class<?> RESULTCONTAINER_CLASS;
    private static final MethodHandle SCTPCHANNELIMPL_RECEIVE0_METHOD;
    private static final MethodHandle RESULTCONTAINER_CLEAR_METHOD;

    static {
        try {
            //only OracleJDK contains SCTP as it seems (causes compile errors in maven/jenkins): https://stackoverflow.com/a/26614215
            //static native int receive0(int fd, ResultContainer resultContainer, long address, int length, boolean peek) throws IOException;
            RESULTCONTAINER_CLASS = Class.forName("sun.nio.ch.sctp.ResultContainer");
            final Class<?> sctpChannelImplClass = Class.forName("sun.nio.ch.sctp.SctpChannelImpl");
            final Method receive0Method = Reflections.findMethod(sctpChannelImplClass, "receive0", int.class,
                    RESULTCONTAINER_CLASS, long.class, int.class, boolean.class);
            Reflections.makeAccessible(receive0Method);
            SCTPCHANNELIMPL_RECEIVE0_METHOD = MethodHandles.lookup().unreflect(receive0Method);

            final Method clearMethod = Reflections.findMethod(RESULTCONTAINER_CLASS, "clear");
            Reflections.makeAccessible(clearMethod);
            RESULTCONTAINER_CLEAR_METHOD = MethodHandles.lookup().unreflect(clearMethod);
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
    private int messageTargetPosition = 0;
    private int fdVal;
    private Object resultContainer;

    public NativeSctpSynchronousReader(final SctpSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        fd = Reflections.getBeanPathValue(channel.getSocketChannel(), "fd");
        fdVal = Reflections.getBeanPathValue(fd, "fd");
        try {
            resultContainer = RESULTCONTAINER_CLASS.getConstructor().newInstance();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
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
            position = 0;
            bufferOffset = 0;
            messageTargetPosition = 0;
            resultContainer = null;
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
            final int sizeTargetPosition = bufferOffset + SctpSynchronousChannel.MESSAGE_INDEX;
            //allow reading further than required to reduce the syscalls if possible
            if (!readFurther(sizeTargetPosition, buffer.remaining(position))) {
                return false;
            }
            final int size = buffer.getInt(bufferOffset + SctpSynchronousChannel.SIZE_INDEX);
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
            position += read0(fdVal, buffer.addressOffset(), position, readLength);
        }
        return position >= targetPosition;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int size = messageTargetPosition - bufferOffset - SctpSynchronousChannel.MESSAGE_INDEX;
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + SctpSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }

        final IByteBuffer message = buffer.slice(bufferOffset + SctpSynchronousChannel.MESSAGE_INDEX, size);
        final int offset = SctpSynchronousChannel.MESSAGE_INDEX + size;
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

    private int read0(final int fd, final long address, final int position, final int length) throws IOException {
        return receive0(fd, resultContainer, address + position, length, false);
    }

    private static int receive0(final int fd, final Object resultContainer, final long address, final int length,
            final boolean peek) throws IOException {
        try {
            RESULTCONTAINER_CLEAR_METHOD.invoke(resultContainer);
            final int res = (int) SCTPCHANNELIMPL_RECEIVE0_METHOD.invoke(fd, resultContainer, address, length, peek);
            if (res == IOStatusAccessor.INTERRUPTED) {
                return 0;
            } else {
                return IOStatusAccessor.normalize(res);
            }
        } catch (final SocketException e) {
            //java.net.SocketException: Ungültiger Dateideskriptor
            throw FastEOFException.getInstance(e);
        } catch (final IOException ioe) {
            throw ioe;
        } catch (final Throwable e) {
            throw new IOException(e);
        }
    }

}
