package de.invesdwin.context.integration.channel.sync.pipe;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
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
    private java.nio.ByteBuffer messageToWrite;
    private int positionBefore;

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
            if (closeMessageEnabled) {
                try {
                    writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
                } catch (final Throwable t) {
                    //ignore
                }
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
            messageToWrite = null;
            positionBefore = 0;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int size = message.getBuffer(messageBuffer);
        buffer.putInt(SIZE_INDEX, size);
        messageToWrite = buffer.asNioByteBuffer(0, MESSAGE_INDEX + size);
        positionBefore = messageToWrite.position();
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (messageToWrite == null) {
            return true;
        } else if (!writeFurther()) {
            ByteBuffers.position(messageToWrite, positionBefore);
            messageToWrite = null;
            positionBefore = 0;
            return true;
        } else {
            return false;
        }
    }

    private boolean writeFurther() throws IOException {
        final int count = fileChannel.write(messageToWrite);
        if (count < 0) { // EOF
            throw ByteBuffers.newEOF();
        }
        return messageToWrite.hasRemaining();
    }

}
