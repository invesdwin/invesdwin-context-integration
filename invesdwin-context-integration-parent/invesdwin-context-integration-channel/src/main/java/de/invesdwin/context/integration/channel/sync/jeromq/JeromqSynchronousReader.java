package de.invesdwin.context.integration.channel.sync.jeromq;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.api.MessageFlag;
import org.zeromq.api.SocketType;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.jeromq.type.IJeromqSocketType;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import zmq.ZError;

@NotThreadSafe
public class JeromqSynchronousReader extends AJeromqSynchronousChannel
        implements ISynchronousReader<IByteBufferProvider> {

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
    public IByteBufferProvider readMessage() throws IOException {
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

    private IByteBuffer getPolledMessage() throws IOException {
        if (polledValue != null) {
            final IByteBuffer value = polledValue;
            polledValue = null;
            return value;
        }
        return poll();
    }

    private IByteBuffer poll() throws IOException {
        final byte[] recv = socket.receive(MessageFlag.DONT_WAIT);
        if (recv == null) {
            if (socket.getZMQSocket().errno() != ZError.EAGAIN) {
                close();
                throw FastEOFException.getInstance("closed by other side");
            }
            return null;
        }
        return ByteBuffers.wrapFrom(recv, topic.length);
    }

}
