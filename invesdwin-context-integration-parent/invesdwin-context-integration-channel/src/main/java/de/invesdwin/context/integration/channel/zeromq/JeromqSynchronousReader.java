package de.invesdwin.context.integration.channel.zeromq;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.api.MessageFlag;
import org.zeromq.api.SocketType;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.zeromq.type.IJeromqSocketType;
import de.invesdwin.util.lang.buffer.ByteBuffers;
import de.invesdwin.util.lang.buffer.ClosedByteBuffer;
import de.invesdwin.util.lang.buffer.IByteBuffer;
import zmq.ZError;

@NotThreadSafe
public class JeromqSynchronousReader extends AJeromqSynchronousChannel implements ISynchronousReader<IByteBuffer> {

    private IByteBuffer polledValue;

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
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer message = getPolledMessage();
        if (message == null || ClosedByteBuffer.isClosed(message)) {
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
            return poll();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private IByteBuffer poll() throws IOException {
        final byte[] recv = socket.receive(MessageFlag.DONT_WAIT);
        if (recv == null) {
            if (socket.getZMQSocket().errno() != ZError.EAGAIN) {
                close();
                throw new EOFException("closed by other side");
            }
            return null;
        }
        return ByteBuffers.wrap(recv);
    }

}