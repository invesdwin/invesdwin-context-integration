package de.invesdwin.context.integration.channel.socket;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.lang.buffer.ClosedByteBuffer;
import de.invesdwin.util.lang.buffer.IByteBuffer;

@NotThreadSafe
public class SocketSynchronousWriter extends ASocketSynchronousChannel implements ISynchronousWriter<IByteBuffer> {

    private BufferedOutputStream out;

    public SocketSynchronousWriter(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        out = new BufferedOutputStream(socket.getOutputStream(), socketSize);
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
    public void write(final IByteBuffer message) throws IOException {
        message.putInt(SIZE_INDEX, message.capacity());
        message.getBytes(MESSAGE_INDEX, out);
        try {
            out.flush();
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

}
