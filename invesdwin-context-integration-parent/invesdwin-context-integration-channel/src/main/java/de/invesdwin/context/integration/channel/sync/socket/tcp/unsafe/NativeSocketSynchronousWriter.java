package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.ASocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;

@NotThreadSafe
public class NativeSocketSynchronousWriter extends ASocketSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private FileDescriptor fd;

    public NativeSocketSynchronousWriter(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        if (socket != null) {
            socket.shutdownInput();
        }
        fd = Jvm.getValue(socketChannel, "fd");
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            fd = null;
            buffer = null;
            messageBuffer = null;
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        try {
            final int size = message.writeBuffer(messageBuffer);
            buffer.putInt(SIZE_INDEX, size);
            writeFully(fd, buffer.addressOffset(), 0, MESSAGE_INDEX + size);
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    public static void writeFully(final FileDescriptor dst, final long address, final int pos, final int length)
            throws IOException {
        int position = pos;
        int remaining = length - pos;
        while (remaining > 0) {
            final int count = write0(dst, address, position, remaining);
            if (count == -1) { // EOF
                break;
            }
            position += count;
            remaining -= count;
        }
        if (remaining > 0) {
            throw ByteBuffers.newPutBytesToEOF();
        }
    }

    public static int write0(final FileDescriptor dst, final long address, final int position, final int length)
            throws IOException {
        return OS.write0(dst, address + position, length);
    }

}
