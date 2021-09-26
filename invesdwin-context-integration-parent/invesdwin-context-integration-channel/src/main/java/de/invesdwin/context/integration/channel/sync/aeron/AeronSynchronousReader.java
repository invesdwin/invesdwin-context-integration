package de.invesdwin.context.integration.channel.sync.aeron;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.delegate.AgronaDelegateByteBuffer;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

@NotThreadSafe
public class AeronSynchronousReader extends AAeronSynchronousChannel implements ISynchronousReader<IByteBuffer> {

    private final AgronaDelegateByteBuffer wrappedBuffer = new AgronaDelegateByteBuffer(
            AgronaDelegateByteBuffer.EMPTY_BYTES);
    private IByteBuffer polledValue;
    private Subscription subscription;

    private final FragmentHandler fragmentHandler = new FragmentAssembler((buffer, offset, length, header) -> {
        wrappedBuffer.setDelegate(buffer);
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
