package de.invesdwin.context.integration.channel.chronicle;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.DocumentContext;

@NotThreadSafe
public class ChronicleSynchronousWriter extends AChronicleSynchronousChannel
        implements ISynchronousWriter<IByteBuffer> {

    private ExcerptAppender appender;

    public ChronicleSynchronousWriter(final File file) {
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
    public void write(final IByteBuffer message) throws IOException {
        try (DocumentContext doc = appender.writingDocument()) {
            final net.openhft.chronicle.bytes.Bytes<?> bytes = doc.wire().bytes();
            final int length = message.capacity();
            for (int i = 0; i < length; i++) {
                bytes.writeByte(i, message.getByte(i));
            }
        }
    }

}
