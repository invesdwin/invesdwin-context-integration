package de.invesdwin.context.integration.channel.sync.fragment;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class FragmentSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private final ISynchronousReader<IByteBuffer> delegate;
    private IByteBuffer combinedBuffer;

    public FragmentSynchronousReader(final ISynchronousReader<IByteBuffer> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        combinedBuffer = ByteBuffers.allocateExpandable();
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
    public IByteBuffer readMessage() throws IOException {
        IByteBuffer buffer = delegate.readMessage();
        final byte fragmentCount = buffer.getByte(FragmentSynchronousWriter.FRAGMENTCOUNT_INDEX);
        if (fragmentCount == 1) {
            //zero copy forward
            return buffer;
        } else {
            int position = 0;
            for (byte i = 1; i < fragmentCount; i++) {
                if (i > 1) {
                    buffer = delegate.readMessage();
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
                combinedBuffer.putBytes(position, buffer, FragmentSynchronousWriter.PAYLOAD_INDEX, payloadLength);
                position += payloadLength;
            }
            return combinedBuffer;
        }
    }

}
