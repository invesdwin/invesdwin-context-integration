package de.invesdwin.context.integration.channel.pipe;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.streams.OutputStreams;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class PipeSynchronousWriter extends APipeSynchronousChannel implements ISynchronousWriter<IByteBufferWriter> {

    private FileOutputStream out;

    public PipeSynchronousWriter(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        out = new FileOutputStream(file, true);
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
                out = null;
            } catch (final Throwable t) {
                //ignore
            }
        }
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
