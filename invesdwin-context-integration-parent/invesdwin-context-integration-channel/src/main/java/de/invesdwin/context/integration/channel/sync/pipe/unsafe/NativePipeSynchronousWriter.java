package de.invesdwin.context.integration.channel.sync.pipe.unsafe;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.APipeSynchronousChannel;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class NativePipeSynchronousWriter extends APipeSynchronousChannel
        implements ISynchronousWriter<IByteBufferProvider> {

    private FileOutputStream out;
    private FileChannel fileChannel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private FileDescriptor fd;
    private long messageToWrite;
    private int position;
    private int remaining;

    public NativePipeSynchronousWriter(final File file, final int estimatedMaxMessageSize) {
        super(file, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        out = new FileOutputStream(file, true);
        fileChannel = out.getChannel();
        fd = Reflections.getBeanPathValue(fileChannel, "fd");
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(fileSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            if (closeMessageEnabled) {
                try {
                    writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
                } catch (final Throwable t) {
                    //ignore
                }
            }
            try {
                out.close();
            } catch (final Throwable t) {
                //ignore
            }
            out = null;
            fileChannel = null;
            buffer = null;
            messageBuffer = null;
            fd = null;
            messageToWrite = 0;
            position = 0;
            remaining = 0;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int size = message.getBuffer(messageBuffer);
        buffer.putInt(SIZE_INDEX, size);
        messageToWrite = buffer.addressOffset();
        position = 0;
        remaining = MESSAGE_INDEX + size;
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
        final int count = write0(fd, messageToWrite, position, remaining);
        remaining -= count;
        position += count;
        return remaining > 0;
    }

    public static int write0(final FileDescriptor dst, final long address, final int position, final int length)
            throws IOException {
        final int res = FileChannelImplAccessor.write0(dst, address + position, length);
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
