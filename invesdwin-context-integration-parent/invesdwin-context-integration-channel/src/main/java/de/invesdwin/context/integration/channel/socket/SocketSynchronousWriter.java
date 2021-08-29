package de.invesdwin.context.integration.channel.socket;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.extend.ExpandableArrayByteBuffer;

@NotThreadSafe
public class SocketSynchronousWriter extends ASocketSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private OutputStream out;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public SocketSynchronousWriter(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        out = socket.getOutputStream();
        buffer = new ExpandableArrayByteBuffer(socketSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            try {
                out.close();
            } catch (final Throwable t) {
                //ignore
            }
            out = null;
            buffer = null;
            messageBuffer = null;
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        try {
            final int size = message.write(messageBuffer);
            buffer.putInt(SIZE_INDEX, size);
            buffer.getBytesTo(0, out, MESSAGE_INDEX + size);
            out.flush();
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

}
