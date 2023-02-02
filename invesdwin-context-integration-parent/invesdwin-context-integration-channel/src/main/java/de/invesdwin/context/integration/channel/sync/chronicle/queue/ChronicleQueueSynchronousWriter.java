package de.invesdwin.context.integration.channel.sync.chronicle.queue;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.ChronicleDelegateByteBuffer;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;

@NotThreadSafe
public class ChronicleQueueSynchronousWriter extends AChronicleQueueSynchronousChannel
        implements ISynchronousWriter<IByteBufferProvider> {

    private ExcerptAppender appender;
    private final ChronicleDelegateByteBuffer wrappedBuffer = new ChronicleDelegateByteBuffer(
            ChronicleDelegateByteBuffer.EMPTY_BYTES);

    public ChronicleQueueSynchronousWriter(final File file) {
        super(file);
    }

    @Override
    public void open() throws IOException {
        super.open();
        appender = queue.acquireAppender();
    }

    @Override
    public void close() throws IOException {
        if (appender != null) {
            writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            appender.close();
            appender = null;
        }
        super.close();
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
            final int position = Integers.checkedCast(bytes.writePosition());
            final int length = message.getBuffer(wrappedBuffer.sliceFrom(position));
            bytes.writePosition(position + length);
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
