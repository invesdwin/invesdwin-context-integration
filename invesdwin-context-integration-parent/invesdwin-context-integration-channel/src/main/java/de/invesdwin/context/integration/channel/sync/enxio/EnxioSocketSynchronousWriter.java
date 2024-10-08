package de.invesdwin.context.integration.channel.sync.enxio;

import java.io.FileDescriptor;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import jnr.enxio.channels.Native;
import jnr.enxio.channels.NativeAccessor;

@NotThreadSafe
public class EnxioSocketSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private SocketSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private FileDescriptor fd;
    private int fdVal;
    private java.nio.ByteBuffer messageToWrite;
    private int position;
    private int remaining;

    public EnxioSocketSynchronousWriter(final SocketSynchronousChannel channel) {
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
            messageToWrite = null;
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
            messageToWrite = buffer.asNioByteBuffer();
            position = 0;
            remaining = SocketSynchronousChannel.MESSAGE_INDEX + size;
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (messageToWrite == null) {
            return true;
        } else if (!writeFurther()) {
            messageToWrite = null;
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

    public static int write0(final int dst, final java.nio.ByteBuffer buffer, final int position, final int length)
            throws IOException {
        final int positionBefore = buffer.position();
        try {
            ByteBuffers.position(buffer, position);
            final int res = NativeAccessor.libc().write(dst, buffer, length);
            if (res < 0) {
                switch (Native.getLastError()) {
                case EAGAIN:
                case EWOULDBLOCK:
                    return 0;
                default:
                    throw new IOException(Native.getLastErrorString());
                }
            } else {
                return res;
            }
        } finally {
            ByteBuffers.position(buffer, positionBefore);
        }
    }

}
