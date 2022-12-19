package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import net.openhft.chronicle.core.Jvm;

@NotThreadSafe
public class NativeDatagramSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    public static final boolean SERVER = true;
    private DatagramSynchronousChannel channel;
    private final int socketSize;
    private IByteBuffer buffer;
    private FileDescriptor fd;
    private int position;
    private final int bufferOffset = 0;

    public NativeDatagramSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this(new DatagramSynchronousChannel(socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public NativeDatagramSynchronousReader(final DatagramSynchronousChannel channel) {
        this.channel = channel;
        if (channel.isServer() != SERVER) {
            throw new IllegalStateException("datagram reader has to be the server");
        }
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        fd = Jvm.getValue(channel.getSocketChannel(), "fd");
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        fd = null;
        position = 0;
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (position > 0) {
            return true;
        }
        final int read = NativeSocketSynchronousReader.read0(fd, buffer.addressOffset(), position,
                buffer.remaining(position));
        if (read < 0) {
            throw FastEOFException.getInstance("socket closed");
        }
        position += read;
        return read > 0;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        if (position > socketSize) {
            throw FastEOFException.getInstance("data truncation occurred");
        }

        final int size = buffer.getInt(DatagramSynchronousChannel.SIZE_INDEX);
        if (size <= 0) {
            close();
            throw FastEOFException.getInstance("non positive size");
        }

        if (ClosedByteBuffer.isClosed(buffer, DatagramSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }

        final IByteBuffer message = buffer.slice(DatagramSynchronousChannel.MESSAGE_INDEX, size);
        position = 0;
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

}
