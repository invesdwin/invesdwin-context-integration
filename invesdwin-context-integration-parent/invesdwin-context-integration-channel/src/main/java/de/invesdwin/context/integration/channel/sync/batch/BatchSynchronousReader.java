package de.invesdwin.context.integration.channel.sync.batch;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BatchSynchronousReader implements ISynchronousReader<IByteBufferProvider>, IByteBufferProvider {

    private final ISynchronousReader<IByteBufferProvider> delegate;
    private IByteBuffer batchMessage;
    private int maxBatchCount;
    private int curBatchCount;
    private int curMessagePosition;
    private int curMessageLength;

    public BatchSynchronousReader(final ISynchronousReader<IByteBufferProvider> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        reset();
    }

    @Override
    public boolean hasNext() throws IOException {
        return batchMessage != null || delegate.hasNext();
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        if (batchMessage != null) {
            return this;
        }
        batchMessage = delegate.readMessage().asBuffer();
        maxBatchCount = batchMessage.getInt(BatchSynchronousWriter.BATCHCOUNT_INDEX);
        curMessagePosition += BatchSynchronousWriter.BATCHCOUNT_SIZE;
        curMessageLength = batchMessage.getInt(curMessagePosition);
        curMessagePosition += BatchSynchronousWriter.PAYLOADLENGTH_SIZE;
        return this;
    }

    @Override
    public void readFinished() throws IOException {
        curBatchCount++;
        if (curBatchCount >= maxBatchCount) {
            reset();
            delegate.readFinished();
        } else {
            curMessagePosition += curMessageLength;
            curMessageLength = batchMessage.getInt(curMessagePosition);
            curMessagePosition += BatchSynchronousWriter.PAYLOADLENGTH_SIZE;
        }
    }

    private void reset() {
        batchMessage = null;
        curMessagePosition = 0;
        curMessageLength = 0;
        maxBatchCount = 0;
        curBatchCount = 0;
    }

    @Override
    public IByteBuffer asBuffer() throws IOException {
        return batchMessage.slice(curMessagePosition, curMessageLength);
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        dst.putBytes(0, asBuffer());
        return curMessageLength;
    }

}
