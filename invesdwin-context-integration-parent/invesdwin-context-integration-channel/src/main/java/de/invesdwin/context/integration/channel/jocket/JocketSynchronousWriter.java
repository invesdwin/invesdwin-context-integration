package de.invesdwin.context.integration.channel.jocket;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class JocketSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

    private final JocketChannel channel;
    private OutputStream out;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public JocketSynchronousWriter(final JocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        out = channel.getSocket().getOutputStream();
        //old socket would actually slow down with direct buffer because it requires a byte[]
        buffer = ByteBuffers.allocateExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, JocketChannel.MESSAGE_INDEX);
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
        channel.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        try {
            final int size = message.write(messageBuffer);
            buffer.putInt(JocketChannel.SIZE_INDEX, size);
            buffer.getBytesTo(0, out, JocketChannel.MESSAGE_INDEX + size);
            out.flush();
        } catch (final IOException e) {
            throw channel.newEofException(e);
        }
    }

}
