package de.invesdwin.context.integration.channel.zeromq;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.api.MessageFlag;
import org.zeromq.api.SocketType;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.zeromq.type.IJeromqSocketType;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.extend.ExpandableArrayByteBuffer;
import de.invesdwin.util.time.date.FTimeUnit;
import zmq.ZError;

@NotThreadSafe
public class JeromqSynchronousWriter extends AJeromqSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private ExpandableArrayByteBuffer buffer;
    private IByteBuffer messageBuffer;

    public JeromqSynchronousWriter(final IJeromqSocketType socketType, final String addr, final boolean server) {
        this(socketType.getWriterSocketType(), addr, server);
    }

    public JeromqSynchronousWriter(final SocketType socketType, final String addr, final boolean server) {
        super(socketType, addr, server);
    }

    @Override
    public void open() throws IOException {
        super.open();
        buffer = new ExpandableArrayByteBuffer();
        if (topic.length > 0) {
            buffer.putBytes(0, topic);
            messageBuffer = new SlicedFromDelegateByteBuffer(buffer, topic.length);
        } else {
            messageBuffer = buffer;
        }
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        final int size = message.write(messageBuffer);
        sendRetrying(size + messageIndex);
    }

    private void sendRetrying(final int size) throws IOException, EOFException, InterruptedIOException {
        while (!sendTry(size)) {
            try {
                FTimeUnit.MILLISECONDS.sleep(1);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                final InterruptedIOException interrupt = new InterruptedIOException(e.getMessage());
                interrupt.initCause(e);
                throw interrupt;
            }
        }
    }

    private boolean sendTry(final int size) throws IOException, EOFException {
        final boolean sent = socket.send(buffer.byteArray(), 0, size, MessageFlag.DONT_WAIT);
        if (!sent) {
            if (socket.getZMQSocket().errno() == ZError.EAGAIN) {
                //backpressure
                return false;
            }
            close();
            throw new EOFException("closed by other side");
        }
        return true;
    }

}
