package de.invesdwin.context.integration.channel.sync.pipe;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.InputStreams;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class PipeSynchronousWriter extends APipeSynchronousChannel implements ISynchronousWriter<IByteBufferProvider> {

    private FileOutputStream out;
    private FileChannel fileChannel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public PipeSynchronousWriter(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        out = new FileOutputStream(file, true);
        fileChannel = out.getChannel();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(fileSize);
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
            fileChannel = null;
            buffer = null;
            messageBuffer = null;
        }
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int size = message.getBuffer(messageBuffer);
        buffer.putInt(SIZE_INDEX, size);
        buffer.getBytesTo(0, fileChannel, MESSAGE_INDEX + size);
    }

    public static void writeFully(final WritableByteChannel dst, final java.nio.ByteBuffer byteBuffer)
            throws IOException {
        int remaining = byteBuffer.remaining();
        final int positionBefore = byteBuffer.position();
        int tries = 0;
        while (remaining > 0) {
            final int count = dst.write(byteBuffer);
            if (count == -1) { // EOF
                break;
            }
            remaining -= count;
            tries++;
            if (tries > InputStreams.MAX_READ_FULLY_TRIES) {
                throw FastEOFException.getInstance("write tries exceeded");
            }
        }
        ByteBuffers.position(byteBuffer, positionBefore);
        if (remaining > 0) {
            throw ByteBuffers.newPutBytesToEOF();
        }
    }

}
