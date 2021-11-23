package de.invesdwin.context.integration.channel.sync.agrona.ringbuffer;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.ringbuffer.RingBuffer;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.time.date.FTimeUnit;

@NotThreadSafe
public class RingBufferSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

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
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
        }
        writer = null;
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        writer.write(message);
    }

    private static final class ExpandableWriter implements IWriter {
        private final RingBuffer ringBuffer;
        private final IByteBuffer buffer;
        private final IByteBuffer messageBuffer;

        private ExpandableWriter(final RingBuffer ringBuffer) {
            this.ringBuffer = ringBuffer;
            this.buffer = ByteBuffers.allocateExpandable();
            this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
        }

        @Override
        public void write(final IByteBufferWriter message) throws IOException {
            final int size = message.writeBuffer(messageBuffer);
            buffer.putInt(SIZE_INDEX, size);
            sendRetrying(MESSAGE_INDEX + size);
        }

        private void sendRetrying(final int size) throws InterruptedIOException {
            while (!sendTry(size)) {
                try {
                    FTimeUnit.MILLISECONDS.sleep(1);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    final InterruptedIOException interrupt = new InterruptedIOException(e.getMessage());
                    interrupt.initCause(e);
                    throw interrupt;
                }
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

        private ZeroCopyWriter(final RingBuffer ringBuffer, final int fixedLength, final int messageFixedLength) {
            this.ringBuffer = ringBuffer;
            this.fixedLength = fixedLength;
            this.maxMessageFixedLength = messageFixedLength;
            this.buffer = ByteBuffers.wrap(ringBuffer.buffer());
        }

        @Override
        public void write(final IByteBufferWriter message) throws IOException {
            while (!sendTry(message)) {
                try {
                    FTimeUnit.MILLISECONDS.sleep(1);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    final InterruptedIOException interrupt = new InterruptedIOException(e.getMessage());
                    interrupt.initCause(e);
                    throw interrupt;
                }
            }
        }

        private boolean sendTry(final IByteBufferWriter message) throws IOException, EOFException {
            final int claimedIndex = ringBuffer.tryClaim(MESSAGE_TYPE_ID, fixedLength);
            if (claimedIndex <= 0) {
                return false;
            }
            final int size = message.writeBuffer(buffer.slice(claimedIndex + MESSAGE_INDEX, maxMessageFixedLength));
            buffer.putInt(claimedIndex + SIZE_INDEX, size);
            ringBuffer.commit(claimedIndex);
            return true;
        }
    }

    private interface IWriter {
        void write(IByteBufferWriter message) throws IOException;
    }

}
