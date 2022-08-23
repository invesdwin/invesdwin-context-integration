package de.invesdwin.context.integration.channel.sync.jeromq;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.api.MessageFlag;
import org.zeromq.api.SocketType;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jeromq.type.IJeromqSocketType;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.extend.ArrayExpandableByteBuffer;
import de.invesdwin.util.time.date.FTimeUnit;
import zmq.ZError;

@NotThreadSafe
public class JeromqSynchronousWriter extends AJeromqSynchronousChannel
        implements ISynchronousWriter<IByteBufferProvider> {

    private ArrayExpandableByteBuffer buffer;
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
        buffer = new ArrayExpandableByteBuffer();
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
    public void write(final IByteBufferProvider message) throws IOException {
        final int size = message.getBuffer(messageBuffer);
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
            throw FastEOFException.getInstance("closed by other side");
        }
        return true;
    }

}
