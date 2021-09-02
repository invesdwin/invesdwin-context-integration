package de.invesdwin.context.integration.channel.aeron;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.time.date.FTimeUnit;
import io.aeron.ConcurrentPublication;
import io.aeron.Publication;

@NotThreadSafe
public class AeronSynchronousWriter extends AAeronSynchronousChannel implements ISynchronousWriter<IByteBufferWriter> {

    private ConcurrentPublication publication;
    private boolean connected;
    private IByteBuffer buffer;

    public AeronSynchronousWriter(final AeronMediaDriverMode mode, final String channel, final int streamId) {
        super(mode, channel, streamId);
    }

    @Override
    public void open() throws IOException {
        super.open();
        this.publication = aeron.addPublication(channel, streamId);
        this.connected = false;
        this.buffer = ByteBuffers.allocateDirectExpandable();
    }

    @Override
    public void close() throws IOException {
        if (publication != null) {
            if (connected) {
                try {
                    write(ClosedByteBuffer.INSTANCE);
                } catch (final Throwable t) {
                    //ignore
                }
            }
            if (publication != null) {
                publication.close();
                publication = null;
                this.connected = false;
            }
            buffer = null;
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        final int size = message.write(buffer);
        sendRetrying(size);
    }

    private void sendRetrying(final int size) throws IOException, EOFException, InterruptedIOException {
        while (!sendTry(size)) {
            try {
                FTimeUnit.MILLISECONDS.sleep(1);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                final InterruptedIOException interrupt = new InterruptedIOException(e.getMessage());
                interrupt.initCause(e);
                throw interrupt;
            }
        }
        connected = true;
    }

    private boolean sendTry(final int size) throws IOException, EOFException {
        final long result = publication.offer(buffer.directBuffer(), 0, size);
        if (result <= 0) {
            if (result == Publication.NOT_CONNECTED) {
                if (connected) {
                    connected = false;
                    close();
                    throw new EOFException("closed by other side: NOT_CONNECTED=" + result);
                } else {
                    return false;
                }
            } else if (result == Publication.CLOSED) {
                close();
                throw new EOFException("closed by other side: CLOSED=" + result);
            } else if (result == Publication.MAX_POSITION_EXCEEDED) {
                close();
                throw new EOFException("closed by other side: MAX_POSITION_EXCEEDED=" + result);
            } else if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
                return false;
            }
        }
        return true;
    }

}
