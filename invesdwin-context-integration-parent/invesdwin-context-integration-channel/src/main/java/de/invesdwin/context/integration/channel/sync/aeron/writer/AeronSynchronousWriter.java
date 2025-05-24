package de.invesdwin.context.integration.channel.sync.aeron.writer;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.aeron.AeronInstance;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.AgronaDelegateMutableByteBuffer;
import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;

@NotThreadSafe
public class AeronSynchronousWriter extends AAeronSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private static final int DISABLED_SIZE = -1;
    protected final int fixedLength;
    protected Publication publication;
    private IByteBuffer buffer;
    private int size;
    private int maxPayloadLength;
    private boolean alwaysFixedLength;
    private BufferClaim bufferClaim;
    private IByteBuffer bufferClaimBuffer;
    private boolean tryClaimResult;

    public AeronSynchronousWriter(final AeronInstance instance, final String channel, final int streamId,
            final Integer fixedLength) {
        super(instance, channel, streamId);
        this.fixedLength = ByteBuffers.newAllocateFixedLength(fixedLength);
    }

    public int getFixedLength() {
        return fixedLength;
    }

    @Override
    public void open() throws IOException {
        super.open();
        this.publication = aeron.addExclusivePublication(channel, streamId);
        this.maxPayloadLength = newMaxPayloadLength(publication);
        this.alwaysFixedLength = isAlwaysFixedLength(fixedLength, maxPayloadLength);
        this.connected = false;
        this.buffer = ByteBuffers.allocateDirectExpandable();
        this.bufferClaim = new BufferClaim();
        this.bufferClaimBuffer = new AgronaDelegateMutableByteBuffer(bufferClaim.buffer())
                .sliceFrom(bufferClaim.offset());
        this.tryClaimResult = false;
        this.size = DISABLED_SIZE;
    }

    protected int newMaxPayloadLength(final Publication publication) {
        return publication.maxPayloadLength();
    }

    protected boolean isAlwaysFixedLength(final int fixedLength, final int maxPayloadLength) {
        return fixedLength != ByteBuffers.EXPANDABLE_LENGTH && maxPayloadLength >= fixedLength;
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
            buffer = null;
            bufferClaim = null;
            bufferClaimBuffer = null;
            tryClaimResult = false;
            size = DISABLED_SIZE;
        }
        super.close();
    }

    @Override
    public boolean writeReady() throws IOException {
        if (alwaysFixedLength) {
            //use fixed length try claim
            if (tryClaimResult) {
                return true;
            }
            final long result = publication.tryClaim(fixedLength, bufferClaim);
            tryClaimResult = handleResult(result);
            return tryClaimResult;
        } else {
            //use dynamic try claim or try offer
            return true;
        }
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        if (alwaysFixedLength) {
            //use fixed try claim
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
        } else {
            //use dynamic try claim or try offer
            this.size = message.getBuffer(buffer);
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (alwaysFixedLength) {
            //use fixed try claim
            return true;
        } else if (size == DISABLED_SIZE) {
            return true;
        } else if (size <= maxPayloadLength) {
            //use dynamic try claim
            final long result = publication.tryClaim(size, bufferClaim);
            if (!handleResult(result)) {
                return false;
            }
            try {
                final int size = buffer.getBuffer(bufferClaimBuffer);
                //tell the other side that the actual message might be smaller than the maxMessageSize that was claimed
                bufferClaim.reservedValue(size);
                bufferClaim.commit();
                return true;
            } catch (final Throwable t) {
                bufferClaim.abort();
                throw t;
            }
        } else {
            //use try offer
            if (tryOffer(publication, buffer, size)) {
                size = DISABLED_SIZE;
                return true;
            } else {
                return false;
            }
        }
    }

}
