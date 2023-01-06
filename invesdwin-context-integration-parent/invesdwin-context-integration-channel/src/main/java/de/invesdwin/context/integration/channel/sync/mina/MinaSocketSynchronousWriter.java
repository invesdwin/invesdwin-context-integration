package de.invesdwin.context.integration.channel.sync.mina;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.buffer.SimpleBufferAllocator;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@NotThreadSafe
public class MinaSocketSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private MinaSocketSynchronousChannel channel;
    private IoBuffer buf;
    private UnsafeByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public MinaSocketSynchronousWriter(final MinaSocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        //netty uses direct buffer per default
        this.buf = new SimpleBufferAllocator().allocate(channel.getSocketSize(), true);
        channel.open(null);
        this.buffer = new UnsafeByteBuffer(buf.buf());
        this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MinaSocketSynchronousChannel.MESSAGE_INDEX);
    }

    @Override
    public void close() {
        if (buffer != null) {
            try {
                writeFuture(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buf.free();
            buf = null;
            buffer = null;
            messageBuffer = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        writeFuture(message);
    }

    private void writeFuture(final IByteBufferProvider message) throws IOException {
        buf.position(0); //reset indexes
        final int size = message.getBuffer(messageBuffer);
        buffer.putInt(MinaSocketSynchronousChannel.SIZE_INDEX, size);
        buf.limit(MinaSocketSynchronousChannel.MESSAGE_INDEX + size);
        channel.getIoSession().write(buf);
    }

}
