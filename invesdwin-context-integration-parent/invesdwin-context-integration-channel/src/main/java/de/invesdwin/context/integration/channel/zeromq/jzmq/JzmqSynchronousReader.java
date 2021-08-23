package de.invesdwin.context.integration.channel.zeromq.jzmq;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.command.ImmutableSynchronousCommand;
import de.invesdwin.context.integration.channel.zeromq.ZeromqFlags;
import de.invesdwin.context.integration.channel.zeromq.jzmq.type.IJzmqSocketType;
import de.invesdwin.util.math.Bytes;

@NotThreadSafe
public class JzmqSynchronousReader extends AJzmqSynchronousChannel implements ISynchronousReader<byte[]> {

    private ImmutableSynchronousCommand<byte[]> polledValue;

    public JzmqSynchronousReader(final IJzmqSocketType socketType, final String addr, final boolean server) {
        this(socketType.getReaderSocketType(), addr, server);
    }

    public JzmqSynchronousReader(final int socketType, final String addr, final boolean server) {
        super(socketType, addr, server);
    }

    @Override
    public boolean hasNext() throws IOException {
        if (polledValue != null) {
            return true;
        }
        polledValue = poll();
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
            return poll();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ImmutableSynchronousCommand<byte[]> poll() throws IOException {
        final byte[] recv = socket.recv(ZeromqFlags.DONTWAIT);
        if (recv == null) {
            //If Socket#recv() returns null and no ZMQException was thrown, an EAGAIN error occurred.
            return null;
        }
        final ByteBuffer buf = ByteBuffer.wrap(recv);
        final int type = buf.getInt(typeIndex);
        final int sequence = buf.getInt(sequenceIndex);
        final int size = recv.length - messageIndex;
        final byte[] message;
        if (size <= 0) {
            message = Bytes.EMPTY_ARRAY;
        } else {
            message = new byte[size];
            buf.get(messageIndex, message);
        }
        return new ImmutableSynchronousCommand<byte[]>(type, sequence, message);
    }

}
