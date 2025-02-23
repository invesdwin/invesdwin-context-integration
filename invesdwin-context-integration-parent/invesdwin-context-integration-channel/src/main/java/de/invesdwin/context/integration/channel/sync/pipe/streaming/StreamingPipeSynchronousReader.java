package de.invesdwin.context.integration.channel.sync.pipe.streaming;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.APipeSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class StreamingPipeSynchronousReader extends APipeSynchronousChannel
        implements ISynchronousReader<IByteBufferProvider>, IByteBufferProvider {

    private FileInputStream in;
    private IByteBuffer buffer;

    public StreamingPipeSynchronousReader(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        in = new FileInputStream(file);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
            buffer = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (in == null) {
            throw FastEOFException.getInstance("already closed");
        }
        try {
            return in.available() >= MESSAGE_INDEX;
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        return this;
    }

    @Override
    public void readFinished() {
        //noop
    }

    @Override
    public IByteBuffer asBuffer() throws IOException {
        if (buffer == null) {
            buffer = ByteBuffers.allocateExpandable(estimatedMaxMessageSize);
        }
        final int length = getBuffer(buffer);
        return buffer.sliceTo(length);
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        try {
            dst.putBytesTo(0, in, MESSAGE_INDEX);
            final int size = dst.getInt(SIZE_INDEX);
            dst.putBytesTo(0, in, size);
            if (closeMessageEnabled) {
                if (ClosedByteBuffer.isClosed(dst, 0, size)) {
                    close();
                    throw FastEOFException.getInstance("closed by other side");
                }
            }
            return size;
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

}
