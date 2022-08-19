package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class SocketSynchronousWriter extends ASocketSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public SocketSynchronousWriter(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        if (socket != null) {
            socket.shutdownInput();
        }
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
            buffer.getBytesTo(0, socketChannel, MESSAGE_INDEX + size);
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

}
