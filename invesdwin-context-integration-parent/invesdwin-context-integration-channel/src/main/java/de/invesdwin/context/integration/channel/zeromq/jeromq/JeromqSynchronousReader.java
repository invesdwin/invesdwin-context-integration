package de.invesdwin.context.integration.channel.zeromq.jeromq;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.SocketType;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.message.EmptySynchronousMessage;
import de.invesdwin.context.integration.channel.message.ISynchronousMessage;
import de.invesdwin.context.integration.channel.message.ImmutableSynchronousMessage;
import de.invesdwin.context.integration.channel.zeromq.ZeromqErrors;
import de.invesdwin.context.integration.channel.zeromq.ZeromqFlags;
import de.invesdwin.context.integration.channel.zeromq.jeromq.type.IJeromqSocketType;
import de.invesdwin.util.math.Bytes;

@NotThreadSafe
public class JeromqSynchronousReader extends AJeromqSynchronousChannel implements ISynchronousReader<byte[]> {

    private ImmutableSynchronousMessage<byte[]> polledValue;

    public JeromqSynchronousReader(final IJeromqSocketType socketType, final String addr, final boolean server) {
        this(socketType.getReaderSocketType(), addr, server);
    }

    public JeromqSynchronousReader(final SocketType socketType, final String addr, final boolean server) {
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
    public ISynchronousMessage<byte[]> readMessage() throws IOException {
        final ISynchronousMessage<byte[]> message = getPolledMessage();
        if (message.getType() == EmptySynchronousMessage.TYPE) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

    private ISynchronousMessage<byte[]> getPolledMessage() {
        if (polledValue != null) {
            final ImmutableSynchronousMessage<byte[]> value = polledValue;
            polledValue = null;
            return value;
        }
        try {
            return poll();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ImmutableSynchronousMessage<byte[]> poll() throws IOException {
        final byte[] recv = socket.recv(ZeromqFlags.DONTWAIT);
        if (recv == null) {
            if (socket.errno() != ZeromqErrors.EAGAIN) {
                close();
                throw new EOFException("closed by other side");
            }
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
        return new ImmutableSynchronousMessage<byte[]>(type, sequence, message);
    }

}