package de.invesdwin.context.integration.channel.agrona;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public class RingBufferSynchronousReader implements ISynchronousReader<IByteBuffer> {

    public static final int SIZE_INDEX = RingBufferSynchronousWriter.SIZE_INDEX;
    public static final int SIZE_SIZE = RingBufferSynchronousWriter.SIZE_SIZE;

    public static final int MESSAGE_INDEX = RingBufferSynchronousWriter.MESSAGE_INDEX;

    private final RingBuffer ringBuffer;
    private final boolean zeroCopy;

    private IReader reader;

    public RingBufferSynchronousReader(final RingBuffer ringBuffer) {
        this(ringBuffer, false);
    }

    /**
     * Using zeroCopy causes reads to be unsafe on small buffer sizes, writers could replace the currently being read
     * value.
     */
    public RingBufferSynchronousReader(final RingBuffer ringBuffer, final boolean zeroCopy) {
        this.ringBuffer = ringBuffer;
        this.zeroCopy = zeroCopy;
    }

    @Override
    public void open() throws IOException {
        if (zeroCopy) {
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
            throw new EOFException("closed by other side");
        }
        return message;
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

    private final class UnsafeReader implements IReader {
        private IByteBuffer wrappedBuffer = EmptyByteBuffer.INSTANCE;

        private IByteBuffer polledValue;

        @Override
        public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index,
                final int length) {
            if (wrappedBuffer.addressOffset() != buffer.addressOffset()
                    || wrappedBuffer.capacity() != buffer.capacity()) {
                wrappedBuffer = ByteBuffers.wrap(buffer);
            }
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

    private final class SafeReader implements IReader {
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
