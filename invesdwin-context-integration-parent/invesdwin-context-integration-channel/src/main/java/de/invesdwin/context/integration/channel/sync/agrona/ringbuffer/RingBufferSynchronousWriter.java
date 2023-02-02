package de.invesdwin.context.integration.channel.sync.agrona.ringbuffer;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.ringbuffer.RingBuffer;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class RingBufferSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    private static final int MESSAGE_TYPE_ID = 1;
    private final RingBuffer ringBuffer;
    private final boolean zeroCopy;
    private final int maxMessageFixedLength;
    private final int fixedLength;
    private IWriter writer;

    /**
     * Use fixedLength = null to disable zero copy
     */
    public RingBufferSynchronousWriter(final RingBuffer ringBuffer, final Integer maxMessageFixedLength) {
        this.ringBuffer = ringBuffer;
        this.maxMessageFixedLength = ByteBuffers.newAllocateFixedLength(maxMessageFixedLength);
        if (this.maxMessageFixedLength > 0) {
            this.zeroCopy = true;
            this.fixedLength = MESSAGE_INDEX + this.maxMessageFixedLength;
        } else {
            this.zeroCopy = false;
            this.fixedLength = ByteBuffers.EXPANDABLE_LENGTH;
        }
    }

    @Override
    public void open() throws IOException {
        if (zeroCopy) {
            this.writer = new ZeroCopyWriter(ringBuffer, fixedLength, maxMessageFixedLength);
        } else {
            this.writer = new ExpandableWriter(ringBuffer);
        }
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            try {
                writeAndFinishIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
        }
        writer = null;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        writer.write(message);
    }

    @Override
    public boolean writeFinished() throws IOException {
        return writer.writeFinished();
    }

    private static final class ExpandableWriter implements IWriter {
        private final RingBuffer ringBuffer;
        private final IByteBuffer buffer;
        private final IByteBuffer messageBuffer;
        private int size = 0;

        private ExpandableWriter(final RingBuffer ringBuffer) {
            this.ringBuffer = ringBuffer;
            this.buffer = ByteBuffers.allocateExpandable();
            this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
        }

        @Override
        public void write(final IByteBufferProvider message) throws IOException {
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(SIZE_INDEX, size);
            this.size = MESSAGE_INDEX + size;
        }

        @Override
        public boolean writeFinished() throws IOException {
            if (size == 0) {
                return true;
            } else if (sendTry(size)) {
                size = 0;
                return true;
            } else {
                return false;
            }
        }

        private boolean sendTry(final int size) {
            final boolean success = ringBuffer.write(MESSAGE_TYPE_ID, buffer.asDirectBuffer(), 0, size);
            return success;
        }
    }

    private static final class ZeroCopyWriter implements IWriter {
        private final RingBuffer ringBuffer;
        private final int fixedLength;
        private final int maxMessageFixedLength;
        private final IByteBuffer buffer;
        private IByteBufferProvider message;

        private ZeroCopyWriter(final RingBuffer ringBuffer, final int fixedLength, final int messageFixedLength) {
            this.ringBuffer = ringBuffer;
            this.fixedLength = fixedLength;
            this.maxMessageFixedLength = messageFixedLength;
            this.buffer = ByteBuffers.wrap(ringBuffer.buffer());
        }

        @Override
        public void write(final IByteBufferProvider message) throws IOException {
            this.message = message;
        }

        @Override
        public boolean writeFinished() throws IOException {
            if (message == null) {
                return true;
            } else if (sendTry(message)) {
                message = null;
                return true;
            } else {
                return false;
            }
        }

        private boolean sendTry(final IByteBufferProvider message) throws IOException, EOFException {
            final int claimedIndex = ringBuffer.tryClaim(MESSAGE_TYPE_ID, fixedLength);
            if (claimedIndex <= 0) {
                return false;
            }
            final int size = message.getBuffer(buffer.slice(claimedIndex + MESSAGE_INDEX, maxMessageFixedLength));
            buffer.putInt(claimedIndex + SIZE_INDEX, size);
            ringBuffer.commit(claimedIndex);
            return true;
        }
    }

    private interface IWriter {
        void write(IByteBufferProvider message) throws IOException;

        boolean writeFinished() throws IOException;
    }

}
