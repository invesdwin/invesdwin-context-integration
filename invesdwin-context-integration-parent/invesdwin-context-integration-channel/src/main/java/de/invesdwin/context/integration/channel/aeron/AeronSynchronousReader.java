package de.invesdwin.context.integration.channel.aeron;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

@NotThreadSafe
public class AeronSynchronousReader extends AAeronSynchronousChannel implements ISynchronousReader<IByteBuffer> {

    private IByteBuffer wrappedBuffer = EmptyByteBuffer.INSTANCE;
    private IByteBuffer polledValue;
    private Subscription subscription;

    private final FragmentHandler fragmentHandler = new FragmentAssembler((buffer, offset, length, header) -> {
        if (wrappedBuffer.addressOffset() != buffer.addressOffset() || wrappedBuffer.capacity() != buffer.capacity()) {
            wrappedBuffer = ByteBuffers.wrap(buffer);
        }
        polledValue = wrappedBuffer.slice(offset, length);
    });

    public AeronSynchronousReader(final AeronMediaDriverMode mode, final String channel, final int streamId) {
        super(mode, channel, streamId);
    }

    @Override
    public void open() throws IOException {
        super.open();
        this.subscription = aeron.addSubscription(channel, streamId);
    }

    @Override
    public void close() throws IOException {
        if (subscription != null) {
            subscription.close();
            subscription = null;
        }
        super.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (polledValue != null) {
            return true;
        }
        subscription.poll(fragmentHandler, 1);
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
            final int fragmentsRead = subscription.poll(fragmentHandler, 1);
            if (fragmentsRead == 1) {
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
