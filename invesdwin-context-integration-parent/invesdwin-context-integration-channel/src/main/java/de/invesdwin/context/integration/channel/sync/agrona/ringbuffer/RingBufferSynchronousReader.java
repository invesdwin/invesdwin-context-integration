package de.invesdwin.context.integration.channel.sync.agrona.ringbuffer;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.delegate.AgronaDelegateByteBuffer;

@NotThreadSafe
public class RingBufferSynchronousReader implements ISynchronousReader<IByteBuffer> {

    public static final int SIZE_INDEX = RingBufferSynchronousWriter.SIZE_INDEX;
    public static final int SIZE_SIZE = RingBufferSynchronousWriter.SIZE_SIZE;

    public static final int MESSAGE_INDEX = RingBufferSynchronousWriter.MESSAGE_INDEX;

    private final RingBuffer ringBuffer;
    private final boolean unsafeRead;

    private IReader reader;

    public RingBufferSynchronousReader(final RingBuffer ringBuffer) {
        this(ringBuffer, false);
    }

    /**
     * WARNING: Using zeroCopy causes reads to be unsafe, writers could replace the currently being read value. If
     * multiple writers are used or if writes should be queued, then zeroCopy should be disabled! The only scenario
     * where it would be safe to use zeroCopy is in a request/reply scenario with two ring buffers.
     * 
     * Though even with safeCopy, RingBuffers do not provide any backpressure, thus too eager writers could override
     * values that have not yet been read by the reader. In those cases it is recommended to use Aeron IPC or different
     * channel implementation instead.
     * 
     * But anyway, ZeroCopy seems to be slightly slower than SafeCopy, so disabling ZeroCopy should not be a problem. It
     * still stays ZeroAllocation, but just does another copy between buffers during reads.
     * 
     * ManyToOneRingBuffer does not work with zero copy reads, so the flag is ignored for that instance.
     */
    public RingBufferSynchronousReader(final RingBuffer ringBuffer, final boolean unsafeRead) {
        this.ringBuffer = ringBuffer;
        this.unsafeRead = !(ringBuffer instanceof ManyToOneRingBuffer) && unsafeRead;
    }

    @Override
    public void open() throws IOException {
        if (unsafeRead) {
            this.reader = new UnsafeReader();
        } else {
            this.reader = new SafeReader();
        }
    }

    @Override
    public void close() throws IOException {
        reader = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (reader.getPolledValue() != null) {
            return true;
        }
        ringBuffer.read(reader, 1);
        return reader.getPolledValue() != null;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer message = getPolledMessage();
        if (message != null && ClosedByteBuffer.isClosed(message)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

    private IByteBuffer getPolledMessage() {
        if (reader.getPolledValue() != null) {
            final IByteBuffer value = reader.getPolledValue();
            reader.close();
            return value;
        }
        try {
            final int messagesRead = ringBuffer.read(reader, 1);
            if (messagesRead == 1) {
                final IByteBuffer value = reader.getPolledValue();
                reader.close();
                return value;
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private interface IReader extends MessageHandler, Closeable {
        IByteBuffer getPolledValue();

        @Override
        void close();

    }

    private static final class UnsafeReader implements IReader {
        private final AgronaDelegateByteBuffer wrappedBuffer = new AgronaDelegateByteBuffer(
                AgronaDelegateByteBuffer.EMPTY_BYTES);

        private IByteBuffer polledValue;

        @Override
        public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index,
                final int length) {
            wrappedBuffer.setDelegate(buffer);
            final int size = wrappedBuffer.getInt(index + SIZE_INDEX);
            polledValue = wrappedBuffer.slice(index + MESSAGE_INDEX, size);
        }

        @Override
        public IByteBuffer getPolledValue() {
            return polledValue;
        }

        @Override
        public void close() {
            polledValue = null;
        }

    }

    private static final class SafeReader implements IReader {
        private final IByteBuffer messageBuffer = ByteBuffers.allocateExpandable();

        private IByteBuffer polledValue;

        @Override
        public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index,
                final int length) {
            final int size = buffer.getInt(index + SIZE_INDEX, ByteBuffers.DEFAULT_ORDER);
            messageBuffer.putBytes(0, buffer, index + MESSAGE_INDEX, size);
            polledValue = messageBuffer.sliceTo(size);
        }

        @Override
        public IByteBuffer getPolledValue() {
            return polledValue;
        }

        @Override
        public void close() {
            polledValue = null;
        }

    }

}
