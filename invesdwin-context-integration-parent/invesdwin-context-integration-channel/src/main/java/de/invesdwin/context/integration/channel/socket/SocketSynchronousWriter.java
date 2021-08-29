package de.invesdwin.context.integration.channel.socket;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.streams.OutputStreams;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class SocketSynchronousWriter extends ASocketSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private OutputStream out;

    public SocketSynchronousWriter(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        out = socket.getOutputStream();
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
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        final IByteBuffer buffer = message.asByteBuffer();
        OutputStreams.writeInt(out, buffer.capacity());
        buffer.getBytes(MESSAGE_INDEX, out);
        try {
            out.flush();
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

}
