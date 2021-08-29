package de.invesdwin.context.integration.channel.socket.nio.udp;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public class NioDatagramSocketSynchronousReader extends ANioDatagramSocketSynchronousChannel
        implements ISynchronousReader<IByteBuffer> {

    protected ByteBuffer sizeBuffer;
    protected int size = 0;
    protected IByteBuffer messageBuffer;

    public NioDatagramSocketSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        super(socketAddress, true, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        sizeBuffer = ByteBuffer.allocateDirect(MESSAGE_INDEX);
        messageBuffer = ByteBuffers.allocateDirectExpandable(socketSize);
        size = 0;
    }

    @Override
    public void close() throws IOException {
        super.close();
        sizeBuffer = null;
        messageBuffer = null;
        size = 0;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (size > 0) {
            return true;
        }
        final int read = socketChannel.read(sizeBuffer);
        if (read > 0 && sizeBuffer.position() == MESSAGE_INDEX) {
            size = sizeBuffer.getInt(0);
            ByteBuffers.position(sizeBuffer, 0);
        }
        return size > 0;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        messageBuffer.putBytesTo(0, socketChannel, size);
        if (ClosedByteBuffer.isClosed(messageBuffer, size)) {
            close();
            throw new EOFException("closed by other side");
        }
        final IByteBuffer message = messageBuffer.sliceTo(size);
        size = 0;
        return message;
    }

}
