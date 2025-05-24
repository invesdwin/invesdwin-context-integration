package de.invesdwin.context.integration.channel.sync.aeron.writer;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.aeron.AeronInstance;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import io.aeron.ConcurrentPublication;

@NotThreadSafe
public class AeronTryOfferSynchronousWriter extends AAeronSynchronousWriter
        implements ISynchronousWriter<IByteBufferProvider> {

    private static final int DISABLED_SIZE = -1;
    private ConcurrentPublication publication;
    private IByteBuffer buffer;
    private int size = DISABLED_SIZE;

    public AeronTryOfferSynchronousWriter(final AeronInstance instance, final String channel, final int streamId) {
        super(instance, channel, streamId);
    }

    @Override
    public void open() throws IOException {
        super.open();
        this.publication = aeron.addPublication(channel, streamId);
        this.connected = false;
        this.buffer = ByteBuffers.allocateDirectExpandable();
        this.size = DISABLED_SIZE;
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
            size = DISABLED_SIZE;
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
        if (size == DISABLED_SIZE) {
            return true;
        } else if (tryOffer(publication, buffer, size)) {
            size = DISABLED_SIZE;
            return true;
        } else {
            return false;
        }
    }

}
