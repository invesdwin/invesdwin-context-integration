package de.invesdwin.context.integration.channel.pipe;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public class PipeSynchronousReader extends APipeSynchronousChannel implements ISynchronousReader<IByteBuffer> {

    protected FileInputStream in;
    protected FileChannel fileChannel;
    protected IByteBuffer buffer;
    protected java.nio.ByteBuffer messageBuffer;

    public PipeSynchronousReader(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        in = new FileInputStream(file);
        fileChannel = in.getChannel();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(fileSize);
        messageBuffer = buffer.asByteBuffer(0, fileSize);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
            fileChannel = null;
            buffer = null;
            messageBuffer = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            //this is a lot faster than directly reading on the channel
            //(i guess because we can not disable blocking mode)
            return in.available() >= MESSAGE_INDEX;
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        ByteBuffers.position(messageBuffer, 0);
        int targetPosition = MESSAGE_INDEX;
        int size = 0;
        //read size
        while (true) {
            final int read = fileChannel.read(messageBuffer);
            if (read < 0) {
                throw new EOFException("closed by other side");
            }
            if (read > 0 && messageBuffer.position() >= targetPosition) {
                size = buffer.getInt(SIZE_INDEX);
                targetPosition += size;
                break;
            }
        }
        //read message if not complete yet
        final int remaining = targetPosition - messageBuffer.position();
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            buffer.putBytesTo(messageBuffer.position(), fileChannel, remaining);
            if (buffer.capacity() != capacityBefore) {
                //update reference to underlying storage
                messageBuffer = buffer.asByteBuffer(0, fileSize);
            }
        }

        if (ClosedByteBuffer.isClosed(buffer, MESSAGE_INDEX, size)) {
            close();
            throw new EOFException("closed by other side");
        }
        return buffer.slice(MESSAGE_INDEX, size);
    }

}
