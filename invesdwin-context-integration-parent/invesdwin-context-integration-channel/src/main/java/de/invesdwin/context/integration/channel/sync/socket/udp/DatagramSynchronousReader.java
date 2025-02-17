package de.invesdwin.context.integration.channel.sync.socket.udp;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
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

    private DatagramSynchronousChannel channel;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private DatagramChannel socketChannel;
    private final int socketSize;
    private final int truncatedSize;

    public DatagramSynchronousReader(final DatagramSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
        this.truncatedSize = socketSize - DatagramSynchronousChannel.MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize + 1);
        messageBuffer = buffer.nioByteBuffer();
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
        if (messageBuffer == null) {
            throw FastEOFException.getInstance("socket closed");
        }
        if (messageBuffer.position() > 0) {
            return true;
        }
        try {
            final SocketAddress receive = socketChannel.receive(messageBuffer);
            if ((channel.isMultipleClientsAllowed() || channel.getOtherSocketAddress() == null) && receive != null) {
                channel.setOtherSocketAddress(receive);
            }
            return messageBuffer.position() > 0;
        } catch (final ClosedChannelException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        if (messageBuffer.position() > socketSize) {
            close();
            throw FastEOFException.getInstance("data truncation occurred: position[%s] > socketSize[%s]",
                    messageBuffer.position(), socketSize);
        }

        final int size = buffer.getInt(DatagramSynchronousChannel.SIZE_INDEX);
        if (size <= 0) {
            close();
            throw FastEOFException.getInstance("non positive size: %s", size);
        }

        if (size > truncatedSize) {
            close();
            throw FastEOFException.getInstance("data truncation occurred: size[%s] > truncatedSize[%s]", size,
                    truncatedSize);
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
