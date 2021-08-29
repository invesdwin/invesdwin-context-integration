package de.invesdwin.context.integration.channel.zeromq;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.api.MessageFlag;
import org.zeromq.api.SocketType;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.socket.udp.ADatagramSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.zeromq.type.IJeromqSocketType;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.time.date.FTimeUnit;
import zmq.ZError;

@NotThreadSafe
public class JeromqSynchronousWriter extends AJeromqSynchronousChannel implements ISynchronousWriter<IByteBuffer> {

    private static final double BUFFER_GROWTH_FACTOR = ADatagramSocketSynchronousChannel.BUFFER_GROWTH_FACTOR;
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
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
        }
        super.close();
    }

    @Override
    public void write(final IByteBuffer message) throws IOException {
        final int size = message.capacity();
        if (bytes.length < size) {
            final int newLength = (int) (size * BUFFER_GROWTH_FACTOR);
            bytes = new byte[newLength];
            buffer = ByteBuffer.wrap(bytes);
            if (topic.length > 0) {
                ByteBuffers.put(buffer, 0, topic);
            }
        }
        message.getBytesTo(0, buffer, size);
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
        final boolean sent = socket.send(bytes, 0, size, MessageFlag.DONT_WAIT);
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
