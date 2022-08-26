package de.invesdwin.context.integration.channel.sync.fragment;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class FragmentSynchronousReader implements ISynchronousReader<IByteBufferProvider>, IByteBufferProvider {

    private final ISynchronousReader<IByteBufferProvider> delegate;
    private IByteBuffer combinedBuffer;

    public FragmentSynchronousReader(final ISynchronousReader<IByteBufferProvider> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        combinedBuffer = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        return this;
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }

    @Override
    public IByteBuffer asBuffer() throws IOException {
        final IByteBuffer buffer = delegate.readMessage().asBuffer();
        final byte fragmentCount = buffer.getByte(FragmentSynchronousWriter.FRAGMENTCOUNT_INDEX);
        if (fragmentCount == 1) {
            //zero copy forward
            return buffer.sliceFrom(FragmentSynchronousWriter.PAYLOAD_INDEX);
        } else {
            if (combinedBuffer == null) {
                combinedBuffer = ByteBuffers.allocateExpandable();
            }
            final int length = fillFragments(combinedBuffer, buffer, fragmentCount);
            return combinedBuffer.sliceTo(length);
        }
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        final IByteBuffer buffer = delegate.readMessage().asBuffer();
        final byte fragmentCount = buffer.getByte(FragmentSynchronousWriter.FRAGMENTCOUNT_INDEX);
        if (fragmentCount == 1) {
            //zero copy forward
            final int length = buffer.remaining(FragmentSynchronousWriter.PAYLOAD_INDEX);
            dst.putBytes(0, buffer, FragmentSynchronousWriter.PAYLOAD_INDEX, length);
            return length;
        } else {
            return fillFragments(dst, buffer, fragmentCount);
        }
    }

    private int fillFragments(final IByteBuffer dst, final IByteBuffer pBuffer, final byte fragmentCount)
            throws IOException {
        IByteBuffer buffer = pBuffer;
        int position = 0;
        for (byte i = 1; i <= fragmentCount; i++) {
            if (i > 1) {
                delegate.readFinished();
                buffer = delegate.readMessage().asBuffer();
            }
            final byte currentFragment = buffer.getByte(FragmentSynchronousWriter.FRAGMENT_INDEX);
            if (currentFragment != i) {
                throw new IllegalStateException("i [" + i + "] != currentFragment [" + currentFragment + "]");
            }
            final int payloadLength = buffer.getInt(FragmentSynchronousWriter.PAYLOADLENGTH_INDEX);
            final int newFragmentCount = buffer.getByte(FragmentSynchronousWriter.FRAGMENTCOUNT_INDEX);
            if (fragmentCount != newFragmentCount) {
                throw new IllegalStateException(
                        "newFragmentCount [" + newFragmentCount + "] != fragmentCount [" + fragmentCount + "]");
            }
            dst.putBytes(position, buffer, FragmentSynchronousWriter.PAYLOAD_INDEX, payloadLength);
            position += payloadLength;
        }
        return position;
    }

}
