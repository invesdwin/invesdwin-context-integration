package de.invesdwin.context.integration.channel.chronicle.queue;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.MemoryDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.memory.ICloseableMemoryBuffer;
import de.invesdwin.util.streams.buffer.memory.delegate.ChronicleDelegateMemoryBuffer;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;

@NotThreadSafe
public class ChronicleQueueSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private ExcerptAppender appender;
    private final ChronicleDelegateMemoryBuffer wrappedBuffer = new ChronicleDelegateMemoryBuffer(
            ChronicleDelegateMemoryBuffer.EMPTY_BYTES, false);
    private final MemoryDelegateByteBuffer wrappedByteBuffer = new MemoryDelegateByteBuffer(wrappedBuffer);
    private final ChronicleQueueSynchronousChannel channel;

    public ChronicleQueueSynchronousWriter(final ChronicleQueueSynchronousChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        appender = channel.getQueue().createAppender();
    }

    @Override
    public void close() throws IOException {
        if (appender != null) {
            writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            appender.close();
            appender = null;
        }
        channel.close();
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        try (DocumentContext doc = appender.writingDocument()) {
            final net.openhft.chronicle.bytes.Bytes<?> bytes = doc.wire().bytes();
            wrappedBuffer.setDelegate(bytes);
            final long position = bytes.writePosition();
            final ICloseableMemoryBuffer slice = wrappedBuffer.sliceFrom(position);
            wrappedByteBuffer.setDelegate(slice);
            final long length = message.getBuffer(wrappedByteBuffer);
            bytes.writePosition(position + length);
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
