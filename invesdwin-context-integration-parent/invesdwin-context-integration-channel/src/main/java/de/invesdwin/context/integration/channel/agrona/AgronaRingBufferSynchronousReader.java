package de.invesdwin.context.integration.channel.agrona;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public class AgronaRingBufferSynchronousReader implements ISynchronousReader<IByteBuffer> {

    public static final int SIZE_INDEX = AgronaRingBufferSynchronousWriter.SIZE_INDEX;
    public static final int SIZE_SIZE = AgronaRingBufferSynchronousWriter.SIZE_SIZE;

    public static final int MESSAGE_INDEX = AgronaRingBufferSynchronousWriter.MESSAGE_INDEX;

    private final RingBuffer ringBuffer;
    private IByteBuffer wrappedBuffer = EmptyByteBuffer.INSTANCE;
    private IByteBuffer polledValue;

    private final MessageHandler messageHandler = (msgTypeId, buffer, index, length) -> {
        if (wrappedBuffer.addressOffset() != buffer.addressOffset() || wrappedBuffer.capacity() != buffer.capacity()) {
            wrappedBuffer = ByteBuffers.wrap(buffer);
        }
        final int size = wrappedBuffer.getInt(index + SIZE_INDEX);
        polledValue = wrappedBuffer.slice(index + MESSAGE_INDEX, size);
    };

    public AgronaRingBufferSynchronousReader(final RingBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        polledValue = null;
        wrappedBuffer = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (polledValue != null) {
            return true;
        }
        ringBuffer.read(messageHandler, 1);
        return polledValue != null;
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
        if (polledValue != null) {
            final IByteBuffer value = polledValue;
            polledValue = null;
            return value;
        }
        try {
            final int messagesRead = ringBuffer.read(messageHandler, 1);
            if (messagesRead == 1) {
                final IByteBuffer value = polledValue;
                polledValue = null;
                return value;
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
