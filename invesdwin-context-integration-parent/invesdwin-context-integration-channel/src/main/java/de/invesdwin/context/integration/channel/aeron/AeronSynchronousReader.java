package de.invesdwin.context.integration.channel.aeron;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.command.ImmutableSynchronousCommand;
import de.invesdwin.util.math.Bytes;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;

@NotThreadSafe
public class AeronSynchronousReader extends AAeronSynchronousChannel implements ISynchronousReader<byte[]> {

    private final FragmentHandler fragmentHandler = new FragmentAssembler((buffer, offset, length, header) -> {
        final int type = buffer.getInt(offset + TYPE_INDEX);
        final int sequence = buffer.getInt(offset + SEQUENCE_INDEX);
        final int size = length - MESSAGE_INDEX;
        final byte[] message;
        if (size <= 0) {
            message = Bytes.EMPTY_ARRAY;
        } else {
            message = new byte[size];
            buffer.getBytes(offset + MESSAGE_INDEX, message);
        }
        polledValue = new ImmutableSynchronousCommand<byte[]>(type, sequence, message);
    });

    private ImmutableSynchronousCommand<byte[]> polledValue;
    private Subscription subscription;

    public AeronSynchronousReader(final String channel, final int streamId) {
        super(channel, streamId);
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
    public ISynchronousCommand<byte[]> readMessage() throws IOException {
        final ISynchronousCommand<byte[]> message = getPolledMessage();
        if (message.getType() == EmptySynchronousCommand.TYPE) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

    private ISynchronousCommand<byte[]> getPolledMessage() {
        if (polledValue != null) {
            final ImmutableSynchronousCommand<byte[]> value = polledValue;
            polledValue = null;
            return value;
        }
        try {
            final int fragmentsRead = subscription.poll(fragmentHandler, 1);
            if (fragmentsRead == 1) {
                final ImmutableSynchronousCommand<byte[]> value = polledValue;
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
