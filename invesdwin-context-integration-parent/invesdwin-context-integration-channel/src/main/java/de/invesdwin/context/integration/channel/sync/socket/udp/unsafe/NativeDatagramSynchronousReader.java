package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.udp.ADatagramSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import net.openhft.chronicle.core.Jvm;

@NotThreadSafe
public class NativeDatagramSynchronousReader extends ADatagramSynchronousChannel
        implements ISynchronousReader<IByteBuffer> {

    public static final boolean SERVER = true;
    private IByteBuffer buffer;
    private FileDescriptor fd;
    private int position;

    public NativeDatagramSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        super(socketAddress, SERVER, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        fd = Jvm.getValue(socketChannel, "fd");
    }

    @Override
    public void close() throws IOException {
        super.close();
        buffer = null;
        fd = null;
        position = 0;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (position > 0) {
            return true;
        }
        final int read = NativeSocketSynchronousReader.read0(fd, buffer.addressOffset(), position, socketSize);
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
            final int read = NativeSocketSynchronousReader.read0(fd, buffer.addressOffset(), position,
                    targetPosition - position);
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
            final int read = NativeSocketSynchronousReader.read0(fd, buffer.addressOffset(), position, remaining);
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

}
