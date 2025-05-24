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
public class AeronTryClaimDynamicSynchronousWriter extends AAeronSynchronousWriter
        implements ISynchronousWriter<IByteBufferProvider> {

    private Publication publication;
    private IByteBuffer bufferClaimBuffer;
    private BufferClaim bufferClaim;
    private IByteBuffer messageBuffer;

    public AeronTryClaimDynamicSynchronousWriter(final AeronInstance instance, final String channel,
            final int streamId) {
        super(instance, channel, streamId);
    }

    @Override
    public void open() throws IOException {
        super.open();
        this.publication = aeron.addExclusivePublication(channel, streamId);
        this.connected = false;
        this.bufferClaim = new BufferClaim();
        this.bufferClaimBuffer = new AgronaDelegateMutableByteBuffer(bufferClaim.buffer())
                .sliceFrom(bufferClaim.offset());
        this.messageBuffer = null;
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
            bufferClaim = null;
            bufferClaimBuffer = null;
            messageBuffer = null;
        }
        super.close();
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        this.messageBuffer = message.asBuffer();
    }

    @Override
    public boolean writeFlushed() throws IOException {
        final long result = publication.tryClaim(messageBuffer.capacity(), bufferClaim);
        if (!handleResult(result)) {
            return false;
        }
        try {
            final int size = messageBuffer.getBuffer(bufferClaimBuffer);
            messageBuffer = null;
            //tell the other side that the actual message might be smaller than the maxMessageSize that was claimed
            bufferClaim.reservedValue(size);
            bufferClaim.commit();
            return true;
        } catch (final Throwable t) {
            bufferClaim.abort();
            throw t;
        }
    }

}
