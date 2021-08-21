package de.invesdwin.context.integration.channel.zeromq.jeromq;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.SocketType;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.message.EmptySynchronousMessage;
import de.invesdwin.context.integration.channel.message.ISynchronousMessage;
import de.invesdwin.context.integration.channel.zeromq.ZeromqErrors;
import de.invesdwin.context.integration.channel.zeromq.ZeromqFlags;
import de.invesdwin.context.integration.channel.zeromq.jeromq.type.IJeromqSocketType;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.time.date.FTimeUnit;

@NotThreadSafe
public class JeromqSynchronousWriter extends AJeromqSynchronousChannel implements ISynchronousWriter<byte[]> {

    private static final double BUFFER_GROWTH_FACTOR = 1.25;
    private byte[] bytes = Bytes.EMPTY_ARRAY;
    private ByteBuffer buffer;

    public JeromqSynchronousWriter(final IJeromqSocketType socketType, final String addr, final boolean server) {
        this(socketType.getWriterSocketType(), addr, server);
    }

    public JeromqSynchronousWriter(final SocketType socketType, final String addr, final boolean server) {
        super(socketType, addr, server);
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            try {
                write(EmptySynchronousMessage.getInstance());
            } catch (final Throwable t) {
                //ignore
            }
        }
        super.close();
    }

    @Override
    public void write(final int type, final int sequence, final byte[] message) throws IOException {
        final int size;
        if (message == null) {
            size = messageIndex;
        } else {
            size = messageIndex + message.length;
        }
        if (bytes.length < size) {
            final int newLength = (int) (size * BUFFER_GROWTH_FACTOR);
            bytes = new byte[newLength];
            buffer = ByteBuffer.wrap(bytes);
            if (topic.length > 0) {
                buffer.put(0, topic);
            }
        }
        buffer.putInt(typeIndex, type);
        buffer.putInt(sequenceIndex, sequence);
        if (message != null) {
            buffer.put(messageIndex, message);
        }
        sendRetrying(size);
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
        final boolean sent = socket.send(bytes, 0, size, ZeromqFlags.DONTWAIT);
        if (!sent) {
            if (socket.errno() == ZeromqErrors.EAGAIN) {
                //backpressure
                return false;
            }
            close();
            throw new EOFException("closed by other side");
        }
        return true;
    }

    @Override
    public void write(final ISynchronousMessage<byte[]> message) throws IOException {
        write(message.getType(), message.getSequence(), message.getMessage());
    }

}
