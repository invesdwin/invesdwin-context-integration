package de.invesdwin.context.integration.channel.sync.pipe.unsafe;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.APipeSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import net.openhft.chronicle.core.Jvm;

@NotThreadSafe
public class NativePipeSynchronousReader extends APipeSynchronousChannel
        implements ISynchronousReader<IByteBufferProvider> {

    private FileInputStream in;
    private FileChannel fileChannel;
    private IByteBuffer buffer;
    private FileDescriptor fd;
    private int position = 0;
    private int bufferOffset = 0;
    private int messageTargetPosition = 0;

    public NativePipeSynchronousReader(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        in = new FileInputStream(file);
        fileChannel = in.getChannel();
        fd = Jvm.getValue(fileChannel, "fd");
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(fileSize);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
            fileChannel = null;
            buffer = null;
            fd = null;
            position = 0;
            bufferOffset = 0;
            messageTargetPosition = 0;
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
                throw FastEOFException.getInstance("non positive size");
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
            final int availableReadLength = Integers.min(available(), readLength);
            if (availableReadLength == 0) {
                return false;
            }
            position += NativeSocketSynchronousReader.read0(fd, buffer.addressOffset(), position, availableReadLength);
        }
        return position >= targetPosition;
    }

    private int available() throws FastEOFException {
        try {
            //this is a lot faster than directly reading on the channel
            //(i guess because we can not disable blocking mode)
            return in.available();
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
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

}
