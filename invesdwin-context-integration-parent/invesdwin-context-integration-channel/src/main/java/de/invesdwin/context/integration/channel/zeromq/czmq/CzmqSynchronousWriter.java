package de.invesdwin.context.integration.channel.zeromq.czmq;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.czmq.Zframe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.zeromq.czmq.type.ICzmqSocketFactory;
import de.invesdwin.context.integration.channel.zeromq.czmq.type.ICzmqSocketType;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.time.date.FTimeUnit;

@NotThreadSafe
public class CzmqSynchronousWriter extends ACzmqSynchronousChannel implements ISynchronousWriter<byte[]> {

    private static final double BUFFER_GROWTH_FACTOR = 1.25;
    private static final int ZFRAME_DONTWAIT = 4;
    private byte[] bytes = Bytes.EMPTY_ARRAY;
    private ByteBuffer buffer;

    public CzmqSynchronousWriter(final ICzmqSocketType socketType, final String addr, final boolean server) {
        this(socketType.newWriterSocketFactory(), addr, server);
    }

    public CzmqSynchronousWriter(final ICzmqSocketFactory socketFactory, final String addr, final boolean server) {
        super(socketFactory, addr, server);
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            try {
                write(EmptySynchronousCommand.getInstance());
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
        try (Zframe frame = new Zframe(bytes, size)) {
            if (frame.self == 0) {
                throw new IOException("no frame");
            }
            final long pointer = frame.self;
            while (!sendTry(frame, pointer)) {
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
    }

    private boolean sendTry(final Zframe frame, final long pointer) throws IOException, EOFException {
        frame.send(socket.self, ZFRAME_DONTWAIT);
        final boolean sent = frame.self == 0;
        if (!sent) {
            if (frame.self == pointer) {
                return false;
            } else {
                close();
                throw new EOFException("closed by other side");
            }
        }
        return true;
    }

    @Override
    public void write(final ISynchronousCommand<byte[]> message) throws IOException {
        write(message.getType(), message.getSequence(), message.getMessage());
    }

}
