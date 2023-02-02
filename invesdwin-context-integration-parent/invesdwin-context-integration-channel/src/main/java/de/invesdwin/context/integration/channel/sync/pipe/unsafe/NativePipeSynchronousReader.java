package de.invesdwin.context.integration.channel.sync.pipe.unsafe;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.APipeSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousReader;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;
import net.openhft.chronicle.core.Jvm;

@NotThreadSafe
public class NativePipeSynchronousReader extends APipeSynchronousChannel
        implements ISynchronousReader<IByteBufferProvider> {

    private FileInputStream in;
    private FileChannel fileChannel;
    private IByteBuffer buffer;
    private FileDescriptor fd;

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
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            //this is a lot faster than directly reading on the channel
            //(i guess because we can not disable blocking mode)
            return in.available() >= MESSAGE_INDEX;
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    //CHECKSTYLE:OFF
    @Override
    public IByteBufferProvider readMessage() throws IOException {
        //CHECKSTYLE:ON
        //System.out.println("TODO non-blocking");
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int position = 0;
        int targetPosition = MESSAGE_INDEX;
        int size = 0;
        //read size
        while (true) {
            final int count = NativeSocketSynchronousReader.read0(fd, buffer.addressOffset(), position,
                    targetPosition - position);
            if (count < 0) {
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
                if (count > 0 && position >= targetPosition) {
                    size = buffer.getInt(SIZE_INDEX);
                    if (size <= 0) {
                        close();
                        throw FastEOFException.getInstance("non positive size");
                    }
                    targetPosition += size;
                    break;
                }
            }
        }
        //read message if not complete yet
        final int remaining = targetPosition - position;
        if (remaining > 0) {
            buffer.ensureCapacity(targetPosition);
            while (position < targetPosition) {
                final int count = NativeSocketSynchronousReader.read0(fd, buffer.addressOffset(), position, remaining);
                if (count < 0) {
                    throw ByteBuffers.newEOF();
                }
                if (count == 0 && timeout != null) {
                    if (zeroCountNanos == -1) {
                        zeroCountNanos = System.nanoTime();
                    } else if (timeout.isLessThanNanos(System.nanoTime() - zeroCountNanos)) {
                        throw FastEOFException.getInstance("read timeout exceeded");
                    }
                    ASpinWait.onSpinWaitStatic();
                } else {
                    zeroCountNanos = -1L;
                    position += count;
                }
            }
        }

        if (ClosedByteBuffer.isClosed(buffer, MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return buffer.slice(MESSAGE_INDEX, size);
    }

    @Override
    public void readFinished() {
        //noop
    }

}
