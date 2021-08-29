package de.invesdwin.context.integration.channel.pipe.stream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.extend.ArrayExpandableByteBuffer;

@NotThreadSafe
public class StreamPipeSynchronousWriter extends AStreamPipeSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private FileOutputStream out;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public StreamPipeSynchronousWriter(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        out = new FileOutputStream(file, true);
        buffer = new ArrayExpandableByteBuffer(fileSize);
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
