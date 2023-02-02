package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;

@NotThreadSafe
public class NativeSocketSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private SocketSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private FileDescriptor fd;
    private long messageToWrite;
    private int position;
    private int remaining;

    public NativeSocketSynchronousWriter(final SocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        if (!channel.isReaderRegistered()) {
            if (channel.getSocket() != null) {
                channel.getSocket().shutdownInput();
            }
        }
        fd = Jvm.getValue(channel.getSocketChannel(), "fd");
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
            remaining = SocketSynchronousChannel.MESSAGE_INDEX + size;
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
        final int count = write0(fd, messageToWrite, position, remaining);
        remaining -= count;
        position += count;
        return remaining > 0;
    }

    public static int write0(final FileDescriptor dst, final long address, final int position, final int length)
            throws IOException {
        final int res = OS.write0(dst, address + position, length);
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
