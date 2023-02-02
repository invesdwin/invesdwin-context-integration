package de.invesdwin.context.integration.channel.sync.aeron;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import io.aeron.ConcurrentPublication;
import io.aeron.Publication;

@NotThreadSafe
public class AeronSynchronousWriter extends AAeronSynchronousChannel
        implements ISynchronousWriter<IByteBufferProvider> {

    private ConcurrentPublication publication;
    private boolean connected;
    private IByteBuffer buffer;
    private int size;

    public AeronSynchronousWriter(final AeronInstance instance, final String channel, final int streamId) {
        super(instance, channel, streamId);
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
                    writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
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
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        this.size = message.getBuffer(buffer);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (size == 0) {
            return true;
        } else if (sendTry(size)) {
            size = 0;
            return true;
        } else {
            return false;
        }
    }

    private boolean sendTry(final int size) throws IOException {
        final long result = publication.offer(buffer.directBuffer(), 0, size);
        if (result <= 0) {
            if (result == Publication.NOT_CONNECTED) {
                if (connected) {
                    connected = false;
                    close();
                    throw FastEOFException.getInstance("closed by other side: NOT_CONNECTED=%s", result);
                } else {
                    return false;
                }
            } else if (result == Publication.CLOSED) {
                close();
                throw FastEOFException.getInstance("closed by other side: CLOSED=%s", result);
            } else if (result == Publication.MAX_POSITION_EXCEEDED) {
                close();
                throw FastEOFException.getInstance("closed by other side: MAX_POSITION_EXCEEDED=%s", result);
            } else if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
                return false;
            }
        }
        connected = true;
        return true;
    }

}
