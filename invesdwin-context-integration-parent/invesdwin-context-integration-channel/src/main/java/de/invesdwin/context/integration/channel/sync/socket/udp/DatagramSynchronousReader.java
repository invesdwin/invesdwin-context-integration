package de.invesdwin.context.integration.channel.sync.socket.udp;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class DatagramSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    public static final boolean SERVER = true;
    private DatagramSynchronousChannel channel;
    private final int socketSize;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private DatagramChannel socketChannel;

    public DatagramSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this(new DatagramSynchronousChannel(socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public DatagramSynchronousReader(final DatagramSynchronousChannel channel) {
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
        buffer = ByteBuffers.allocateDirectExpandable(socketSize + 1);
        messageBuffer = buffer.asNioByteBuffer(0, buffer.capacity());
        socketChannel = channel.getSocketChannel();
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        messageBuffer = null;
        socketChannel = null;
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (messageBuffer.position() > 0) {
            return true;
        }
        final SocketAddress client = socketChannel.receive(messageBuffer);
        return client != null;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        if (messageBuffer.position() > socketSize) {
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
        ByteBuffers.position(messageBuffer, 0);
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

}
