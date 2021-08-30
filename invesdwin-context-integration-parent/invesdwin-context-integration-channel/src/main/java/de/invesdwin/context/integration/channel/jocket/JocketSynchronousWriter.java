package de.invesdwin.context.integration.channel.jocket;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import jocket.impl.JocketWriter;

@NotThreadSafe
public class JocketSynchronousWriter extends AJocketSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private JocketWriter writer;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public JocketSynchronousWriter(final int port, final boolean server, final int estimatedMaxMessageSize) {
        super(port, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        writer = socket.getWriter();
        //old socket would actually slow down with direct buffer because it requires a byte[]
        buffer = ByteBuffers.allocateExpandable(socketSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            try {
                writer.close();
            } catch (final Throwable t) {
                //ignore
            }
            writer = null;
            buffer = null;
            messageBuffer = null;
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        final int size = message.write(messageBuffer);
        buffer.putInt(SIZE_INDEX, size);
        writeFully(buffer.byteArray(), 0, size);
    }

    private void writeFully(final byte[] b, final int off, final int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }
        int n = 0;
        while (n < len) {
            final int count = writer.write(b, off + n, len - n);
            if (count < 0) {
                throw new EOFException();
            }
            n += count;
        }
    }

}
