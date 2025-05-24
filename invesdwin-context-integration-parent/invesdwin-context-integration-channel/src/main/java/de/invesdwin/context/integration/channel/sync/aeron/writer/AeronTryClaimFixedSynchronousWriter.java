package de.invesdwin.context.integration.channel.sync.aeron.writer;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.aeron.AeronInstance;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.AgronaDelegateMutableByteBuffer;
import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;

@NotThreadSafe
public class AeronTryClaimFixedSynchronousWriter extends AAeronSynchronousWriter
        implements ISynchronousWriter<IByteBufferProvider> {

    private Publication publication;
    private final int maxMessageSize;
    private BufferClaim bufferClaim;
    private IByteBuffer bufferClaimBuffer;
    private boolean tryClaimResult;

    public AeronTryClaimFixedSynchronousWriter(final AeronInstance instance, final String channel, final int streamId,
            final int maxMessageSize) {
        super(instance, channel, streamId);
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public void open() throws IOException {
        super.open();
        this.publication = aeron.addExclusivePublication(channel, streamId);
        if (publication.maxPayloadLength() < maxMessageSize) {
            throw new IllegalStateException("maxMessageSize [" + maxMessageSize
                    + "] is bigger than publication.maxPayloadLength [" + publication.maxPayloadLength() + "]");
        }
        this.connected = false;
        this.bufferClaim = new BufferClaim();
        this.bufferClaimBuffer = new AgronaDelegateMutableByteBuffer(bufferClaim.buffer())
                .sliceFrom(bufferClaim.offset());
        tryClaimResult = false;
    }

    @Override
    public void close() throws IOException {
        if (publication != null) {
            if (connected) {
                if (writeReady()) {
                    try {
                        writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
                    } catch (final Throwable t) {
                        //ignore
                    }
                }
            }
            if (publication != null) {
                publication.close();
                publication = null;
                this.connected = false;
            }
            bufferClaim = null;
            bufferClaimBuffer = null;
            tryClaimResult = false;
        }
        super.close();
    }

    @Override
    public boolean writeReady() throws IOException {
        if (tryClaimResult) {
            return true;
        }
        final long result = publication.tryClaim(maxMessageSize, bufferClaim);
        tryClaimResult = handleResult(result);
        return tryClaimResult;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        try {
            final int size = message.getBuffer(bufferClaimBuffer);
            //tell the other side that the actual message might be smaller than the maxMessageSize that was claimed
            bufferClaim.reservedValue(size);
            bufferClaim.commit();
        } catch (final Throwable t) {
            bufferClaim.abort();
            throw t;
        } finally {
            tryClaimResult = false;
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
