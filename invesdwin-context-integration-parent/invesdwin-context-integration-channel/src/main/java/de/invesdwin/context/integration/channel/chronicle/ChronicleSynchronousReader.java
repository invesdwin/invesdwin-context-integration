package de.invesdwin.context.integration.channel.chronicle;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.ChronicleDelegateByteBuffer;
import net.openhft.chronicle.queue.ExcerptTailer;

@NotThreadSafe
public class ChronicleSynchronousReader extends AChronicleSynchronousChannel
        implements ISynchronousReader<IByteBuffer> {

    private ExcerptTailer tailer;
    private net.openhft.chronicle.bytes.Bytes<?> bytes;
    private IByteBuffer buffer;

    public ChronicleSynchronousReader(final File file) {
        super(file);
    }

    @Override
    public void open() throws IOException {
        super.open();
        this.tailer = queue.createTailer();
        //chronicle uses direct buffers per default
        this.bytes = net.openhft.chronicle.bytes.Bytes.elasticByteBuffer();
        this.buffer = new ChronicleDelegateByteBuffer(bytes);
    }

    @Override
    public void close() throws IOException {
        if (tailer != null) {
            tailer.close();
            tailer = null;
            bytes = null;
            buffer = null;
        }
        super.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        return tailer.readBytes(bytes);
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final int length = (int) bytes.writePosition();
        if (ClosedByteBuffer.isClosed(buffer, 0, length)) {
            close();
            throw new EOFException("closed by other side");
        }
        bytes.writePosition(0);
        return buffer.slice(0, length);
    }

}
