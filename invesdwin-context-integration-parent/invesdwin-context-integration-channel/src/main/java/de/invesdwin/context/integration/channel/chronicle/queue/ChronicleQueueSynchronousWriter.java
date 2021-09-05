package de.invesdwin.context.integration.channel.chronicle.queue;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.ChronicleDelegateByteBuffer;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;

@NotThreadSafe
public class ChronicleQueueSynchronousWriter extends AChronicleQueueSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

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
            write(ClosedByteBuffer.INSTANCE);
            appender.close();
            appender = null;
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        try (DocumentContext doc = appender.writingDocument()) {
            final net.openhft.chronicle.bytes.Bytes<?> bytes = doc.wire().bytes();
            wrappedBuffer.setDelegate(bytes);
            final int position = Integers.checkedCast(bytes.writePosition());
            final int length = message.write(wrappedBuffer.sliceFrom(position));
            bytes.writePosition(position + length);
        }
    }

}
