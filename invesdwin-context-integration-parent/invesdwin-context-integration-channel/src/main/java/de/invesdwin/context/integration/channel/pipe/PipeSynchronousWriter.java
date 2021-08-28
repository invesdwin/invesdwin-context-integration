package de.invesdwin.context.integration.channel.pipe;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.lang.buffer.ClosedByteBuffer;
import de.invesdwin.util.lang.buffer.IByteBuffer;

@NotThreadSafe
public class PipeSynchronousWriter extends APipeSynchronousChannel implements ISynchronousWriter<IByteBuffer> {

    private BufferedOutputStream out;

    public PipeSynchronousWriter(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        out = new BufferedOutputStream(new FileOutputStream(file, true), fileSize);
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
