package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.ASocketSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;

@NotThreadSafe
public class NativeSocketSynchronousReader extends ASocketSynchronousChannel
        implements ISynchronousReader<IByteBuffer> {

    private IByteBuffer buffer;
    private FileDescriptor fd;
    private int position = 0;

    public NativeSocketSynchronousReader(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        if (socket != null) {
            socket.shutdownOutput();
        }
        fd = Jvm.getValue(socketChannel, "fd");
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        position = 0;
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer = null;
            fd = null;
        }
        super.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (position > 0) {
            return true;
        }
        final int read = read0(fd, buffer.addressOffset(), position, socketSize);
        if (read < 0) {
            throw new EOFException("socket closed");
        }
        position += read;
        return read > 0;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        int targetPosition = MESSAGE_INDEX;
        int size = 0;
        //read size
        while (position < targetPosition) {
            final int read = read0(fd, buffer.addressOffset(), position, targetPosition - position);
            if (read < 0) {
                throw new EOFException("socket closed");
            }
            position += read;
        }
        size = buffer.getInt(SIZE_INDEX);
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - position;
        if (remaining > 0) {
            buffer.ensureCapacity(targetPosition);
            final int read = read0(fd, buffer.addressOffset(), position, remaining);
            if (read < 0) {
                throw new EOFException("socket closed");
            }
            position += read;
        }

        position = 0;
        if (ClosedByteBuffer.isClosed(buffer, MESSAGE_INDEX, size)) {
            close();
            throw new EOFException("closed by other side");
        }
        return buffer.slice(MESSAGE_INDEX, size);
    }

    @Override
    public void readFinished() {
        //noop
    }

    public static int read0(final FileDescriptor src, final long address, final int position, final int length)
            throws IOException {
        final int res = OS.read0(src, address + position, length);
        if (res == IOTools.IOSTATUS_INTERRUPTED) {
            return 0;
        } else {
            return IOTools.normaliseIOStatus(res);
        }
    }

}
