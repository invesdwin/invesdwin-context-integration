package de.invesdwin.context.integration.channel.sync.fragment;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.math.Doubles;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class FragmentSynchronousWriter implements ISynchronousWriter<IByteBufferProvider>, IByteBufferProvider {

    public static final int FRAGMENTCOUNT_INDEX = 0;
    public static final int FRAGMENTCOUNT_SIZE = Byte.BYTES;

    public static final int FRAGMENT_INDEX = FRAGMENTCOUNT_INDEX + FRAGMENTCOUNT_SIZE;
    public static final int FRAGMENT_SIZE = Byte.BYTES;

    public static final int PAYLOADLENGTH_INDEX = FRAGMENT_INDEX + FRAGMENT_SIZE;
    public static final int PAYLOADLENGTH_SIZE = Integer.BYTES;

    public static final int PAYLOAD_INDEX = PAYLOADLENGTH_INDEX + PAYLOADLENGTH_SIZE;

    private final ISynchronousWriter<IByteBufferProvider> delegate;
    private final int maxMessageLength;
    private final int maxPayloadLength;
    private IByteBuffer buffer;

    private IByteBuffer message;
    private byte fragmentCount;
    private byte currentFragment = 1;
    private int currentPosition;

    public FragmentSynchronousWriter(final ISynchronousWriter<IByteBufferProvider> delegate,
            final int maxMessageLength) {
        this.delegate = delegate;
        this.maxMessageLength = maxMessageLength;
        this.maxPayloadLength = maxMessageLength - PAYLOAD_INDEX;
        if (maxPayloadLength <= 0) {
            throw new IllegalArgumentException("insufficient maxPayloadLength[" + maxPayloadLength
                    + "] from maxMessageLength[" + maxMessageLength + "] - headerLength[" + PAYLOAD_INDEX + "] ");
        }
    }

    public int getMaxMessageLength() {
        return maxMessageLength;
    }

    public ISynchronousWriter<IByteBufferProvider> getDelegate() {
        return delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        buffer = null;
    }

    @Override
    public boolean writeReady() throws IOException {
        return delegate.writeReady();
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        this.message = message.asBuffer();
        final double fragmentCountDouble = Doubles.divide(this.message.capacity(), maxPayloadLength);
        final double fragmentCountDoubleRounded = Math.ceil(fragmentCountDouble);
        if (fragmentCountDouble > Byte.MAX_VALUE) {
            throw new IllegalStateException("fragmentCount [" + fragmentCountDoubleRounded + "] should not exceed ["
                    + Byte.MAX_VALUE + "]. Please increase the maxMessageLength [" + maxMessageLength
                    + "] so that less fragments can be used for efficient delivery of messageLength ["
                    + this.message.capacity() + "]");
        }
        if (fragmentCountDoubleRounded <= 0D) {
            throw new IllegalStateException(
                    "fragmentCount [" + fragmentCountDoubleRounded + "] should be greater than 0");
        }
        //if there are more than 127 fragments, then the maxMessageLength should be increased so that
        fragmentCount = Bytes.checkedCast(fragmentCountDoubleRounded);
        currentPosition = 0;
        currentFragment = 1;
        delegate.write(this);
        currentFragment++;
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (!delegate.writeFlushed()) {
            return false;
        } else if (currentFragment > fragmentCount) {
            this.message = null;
            return true;
        } else if (!delegate.writeReady()) {
            return false;
        } else {
            delegate.write(this);
            currentFragment++;
            return false;
        }
    }

    @Override
    public int getBuffer(final IByteBuffer dst) {
        dst.putByte(FRAGMENTCOUNT_INDEX, fragmentCount);
        dst.putByte(FRAGMENT_INDEX, currentFragment);
        final int payloadLength = Integers.min(maxPayloadLength, message.remaining(currentPosition));
        if (payloadLength == 0) {
            throw new IllegalStateException("payloadLength should be positive: " + payloadLength);
        }
        dst.putInt(PAYLOADLENGTH_INDEX, payloadLength);
        dst.putBytes(PAYLOAD_INDEX, message.slice(currentPosition, payloadLength));
        currentPosition += payloadLength;
        final int length = PAYLOAD_INDEX + payloadLength;
        return length;
    }

    @Override
    public IByteBuffer asBuffer() {
        if (buffer == null) {
            buffer = ByteBuffers.allocate(this.maxMessageLength);
        }
        final int length = getBuffer(buffer);
        return buffer.slice(0, length);
    }

}
