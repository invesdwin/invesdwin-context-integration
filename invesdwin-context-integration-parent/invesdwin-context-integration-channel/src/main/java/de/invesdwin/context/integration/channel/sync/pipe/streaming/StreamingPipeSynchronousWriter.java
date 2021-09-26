package de.invesdwin.context.integration.channel.sync.pipe.streaming;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.extend.ArrayExpandableByteBuffer;

@NotThreadSafe
public class StreamingPipeSynchronousWriter extends AStreamingPipeSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private FileOutputStream out;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public StreamingPipeSynchronousWriter(final File file, final int maxMessageSize) {
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
