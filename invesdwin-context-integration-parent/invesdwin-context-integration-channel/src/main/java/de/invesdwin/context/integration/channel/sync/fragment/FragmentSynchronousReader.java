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
    private IByteBuffer combinedMessage;
    private int position;
    private int fragmentCount;
    private int currentFragment;

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
        reset();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (combinedMessage != null) {
            return true;
        }
        readFurther();
        return combinedMessage != null;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        return this;
    }

    @Override
    public void readFinished() {
        reset();
    }

    private void reset() {
        combinedMessage = null;
        position = 0;
        currentFragment = 0;
        fragmentCount = 0;
    }

    @Override
    public IByteBuffer asBuffer() throws IOException {
        return combinedMessage;
    }

    private void readFurther() throws IOException {
        if (!delegate.hasNext()) {
            return;
        }
        final IByteBuffer buffer = delegate.readMessage().asBuffer();
        if (fragmentCount == 0) {
            currentFragment = 1;
            fragmentCount = buffer.getByte(FragmentSynchronousWriter.FRAGMENTCOUNT_INDEX);
            if (fragmentCount == 1) {
                //zero copy forward
                combinedMessage = buffer.sliceFrom(FragmentSynchronousWriter.PAYLOAD_INDEX);
            } else {
                //initialize combining
                if (combinedBuffer == null) {
                    combinedBuffer = ByteBuffers.allocateExpandable();
                }
                fillFragments(combinedBuffer, buffer);
            }
        } else {
            //continue filling
            fillFragments(combinedBuffer, buffer);
        }
        delegate.readFinished();
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        final IByteBuffer combinedMessage = asBuffer();
        dst.putBytes(0, combinedMessage);
        return combinedMessage.capacity();
    }

    private void fillFragments(final IByteBuffer dst, final IByteBuffer buffer) throws IOException {
        final byte newCurrentFragment = buffer.getByte(FragmentSynchronousWriter.FRAGMENT_INDEX);
        if (newCurrentFragment != currentFragment) {
            throw new IllegalStateException(
                    "newCurrentFragment [" + newCurrentFragment + "] != currentFragment [" + currentFragment + "]");
        }
        final int payloadLength = buffer.getInt(FragmentSynchronousWriter.PAYLOADLENGTH_INDEX);
        final int newFragmentCount = buffer.getByte(FragmentSynchronousWriter.FRAGMENTCOUNT_INDEX);
        if (fragmentCount != newFragmentCount) {
            throw new IllegalStateException(
                    "newFragmentCount [" + newFragmentCount + "] != fragmentCount [" + fragmentCount + "]");
        }
        dst.putBytes(position, buffer, FragmentSynchronousWriter.PAYLOAD_INDEX, payloadLength);
        position += payloadLength;
        currentFragment++;

        if (currentFragment > fragmentCount) {
            combinedMessage = combinedBuffer.sliceTo(position);
        }
    }

}
