package de.invesdwin.context.integration.channel.sync.disni.active;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.verbs.SVCPostSend;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@NotThreadSafe
public class DisniActiveSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private DisniActiveSynchronousChannel channel;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer nioBuffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private long messageToWrite;
    private SVCPostSend sendTask;
    private boolean request;

    public DisniActiveSynchronousWriter(final DisniActiveSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        nioBuffer = channel.getEndpoint().getSendBuf();
        buffer = new UnsafeByteBuffer(nioBuffer);
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, DisniActiveSynchronousChannel.MESSAGE_INDEX);

        sendTask = channel.getEndpoint().postSend(channel.getEndpoint().getWrList_send());
    }

    @Override
    public void close() {
        if (buffer != null) {
            sendTask.free();
            sendTask = null;
            request = false;

            try {
                writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
            nioBuffer = null;
            messageBuffer = null;
            messageToWrite = 0;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        try {
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(DisniActiveSynchronousChannel.SIZE_INDEX, size);
            messageToWrite = buffer.addressOffset();
            ByteBuffers.position(nioBuffer, 0);
            ByteBuffers.limit(nioBuffer, DisniActiveSynchronousChannel.MESSAGE_INDEX + size);
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (messageToWrite == 0) {
            return true;
        } else if (!writeFurther()) {
            messageToWrite = 0;
            nioBuffer.clear();
            return true;
        } else {
            return false;
        }
    }

    private boolean writeFurther() throws IOException {
        if (!request) {
            sendTask.execute();
            request = true;
        }
        if (!channel.getEndpoint().isSendFinished()) {
            return true;
        }
        channel.getEndpoint().setSendFinished(false);
        request = false;
        return false;
    }

}
