package de.invesdwin.context.integration.channel.chronicle.queue;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.ChronicleDelegateByteBuffer;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;

@NotThreadSafe
public class ChronicleQueueSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private ExcerptTailer tailer;
    private net.openhft.chronicle.bytes.Bytes<?> bytes;
    private IByteBuffer buffer;
    private final ChronicleQueueSynchronousChannel channel;

    public ChronicleQueueSynchronousReader(final ChronicleQueueSynchronousChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        this.tailer = channel.getQueue().createTailer();
        //chronicle uses direct buffers per default
        this.bytes = net.openhft.chronicle.bytes.Bytes.elasticByteBuffer();
        this.buffer = new ChronicleDelegateByteBuffer(bytes, false);
    }

    @Override
    public void close() throws IOException {
        if (tailer != null) {
            tailer.close();
            tailer = null;
            bytes.releaseLast();
            bytes = null;
            buffer = null;
        }
        channel.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        try (DocumentContext doc = tailer.readingDocument()) {
            if (!doc.isPresent()) {
                return false;
            }
            final net.openhft.chronicle.bytes.Bytes<?> wireBytes = doc.wire().bytes();
            bytes.clear();
            bytes.write(wireBytes);
            return true;
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int length = Integers.checkedCast(bytes.writePosition());
        if (ClosedByteBuffer.isClosed(buffer, 0, length)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        bytes.writePosition(0);
        return buffer.slice(0, length);
    }

    @Override
    public void readFinished() {
        //noop
    }

}
