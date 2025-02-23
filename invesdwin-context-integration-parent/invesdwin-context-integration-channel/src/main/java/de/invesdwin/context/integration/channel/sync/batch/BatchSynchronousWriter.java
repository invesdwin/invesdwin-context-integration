package de.invesdwin.context.integration.channel.sync.batch;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class BatchSynchronousWriter implements ISynchronousWriter<IByteBufferProvider>, IByteBufferProvider {

    public static final int BATCHCOUNT_INDEX = 0;
    public static final int BATCHCOUNT_SIZE = Integer.BYTES;

    public static final int PAYLOADLENGTH_INDEX = BATCHCOUNT_INDEX + BATCHCOUNT_SIZE;
    public static final int PAYLOADLENGTH_SIZE = Integer.BYTES;

    public static final int PAYLOAD_INDEX = PAYLOADLENGTH_INDEX + PAYLOADLENGTH_SIZE;

    private final ISynchronousWriter<IByteBufferProvider> delegate;
    private final int maxBatchMessageLength;
    private final int maxBatchMessageCount;
    private final Duration maxBatchMessageInterval;

    private long batchStartNanos;
    private int batchCount;
    private int batchBufferPosition;
    private IByteBuffer batchBuffer;

    private IByteBuffer exceedingMessage;

    public BatchSynchronousWriter(final ISynchronousWriter<IByteBufferProvider> delegate,
            final int maxBatchMessageLength, final int maxBatchMessageCount, final Duration maxBatchMessageInterval) {
        this.delegate = delegate;
        this.maxBatchMessageLength = maxBatchMessageLength;
        this.maxBatchMessageCount = maxBatchMessageCount;
        this.maxBatchMessageInterval = maxBatchMessageInterval;
    }

    public int getMaxBatchMessageLength() {
        return maxBatchMessageLength;
    }

    public ISynchronousWriter<IByteBufferProvider> getDelegate() {
        return delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        this.batchBuffer = ByteBuffers.allocateDirectExpandable(maxBatchMessageLength);
        reset();
    }

    @Override
    public void close() throws IOException {
        flushBatch();
        delegate.writeFlushed();
        delegate.close();
        batchBuffer = null;
        reset();
    }

    private void reset() {
        batchBufferPosition = PAYLOADLENGTH_INDEX;
        batchCount = 0;
        batchStartNanos = System.nanoTime();
    }

    @Override
    public boolean writeReady() throws IOException {
        final boolean ready = exceedingMessage == null && delegate.writeReady();
        if (ready && batchCount > 0 && shouldFlushBatchByTime()) {
            flushBatch();
            return false;
        } else {
            return ready;
        }
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final IByteBuffer nextMessage = message.asBuffer();
        final int payloadLength = nextMessage.capacity();
        final int nextPosition = batchBufferPosition + PAYLOADLENGTH_SIZE + payloadLength;
        if (batchCount == 0 || nextPosition <= maxBatchMessageLength) {
            batchBuffer.putInt(batchBufferPosition, payloadLength);
            batchBuffer.putBytes(batchBufferPosition + PAYLOADLENGTH_SIZE, nextMessage);
            batchBufferPosition = nextPosition;
            batchCount++;
            if (shouldFlushBatch()) {
                flushBatch();
            }
        } else {
            exceedingMessage = nextMessage;
            flushBatch();
        }
    }

    protected boolean shouldFlushBatch() {
        if (batchCount >= maxBatchMessageCount) {
            return true;
        }
        if (batchBufferPosition >= maxBatchMessageLength) {
            return true;
        }
        return shouldFlushBatchByTime();
    }

    protected boolean shouldFlushBatchByTime() {
        if (maxBatchMessageInterval != null
                && maxBatchMessageInterval.isLessThanOrEqualToNanos(System.nanoTime() - batchStartNanos)) {
            return true;
        }
        return false;
    }

    private void flushBatch() throws IOException {
        batchBuffer.putInt(BATCHCOUNT_INDEX, batchCount);
        delegate.write(this);
        reset();
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (!delegate.writeFlushed()) {
            return false;
        } else if (!delegate.writeReady()) {
            return false;
        } else {
            final IByteBuffer exceedingMessageCopy = exceedingMessage;
            if (exceedingMessageCopy != null) {
                exceedingMessage = null;
                //add exceeding message to next batch since current batch finished writing
                write(exceedingMessageCopy);
            }
            return true;
        }
    }

    @Override
    public int getBuffer(final IByteBuffer dst) {
        dst.putBytes(0, batchBuffer, 0, batchBufferPosition);
        return batchBufferPosition;
    }

    @Override
    public IByteBuffer asBuffer() {
        return batchBuffer.slice(0, batchBufferPosition);
    }

}
