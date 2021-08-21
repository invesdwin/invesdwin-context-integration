package de.invesdwin.context.integration.channel.zeromq.czmq;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.czmq.Zframe;
import org.zeromq.czmq.Zpoller;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.message.EmptySynchronousMessage;
import de.invesdwin.context.integration.channel.message.ISynchronousMessage;
import de.invesdwin.context.integration.channel.message.ImmutableSynchronousMessage;
import de.invesdwin.context.integration.channel.zeromq.czmq.type.ICzmqSocketFactory;
import de.invesdwin.context.integration.channel.zeromq.czmq.type.ICzmqSocketType;
import de.invesdwin.util.math.Bytes;

@NotThreadSafe
public class CzmqSynchronousReader extends ACzmqSynchronousChannel implements ISynchronousReader<byte[]> {

    private ImmutableSynchronousMessage<byte[]> polledValue;
    private Zpoller poller;

    public CzmqSynchronousReader(final ICzmqSocketType socketType, final String addr, final boolean server) {
        this(socketType.newReaderSocketFactory(), addr, server);
    }

    public CzmqSynchronousReader(final ICzmqSocketFactory socketFactory, final String addr, final boolean server) {
        super(socketFactory, addr, server);
    }

    @Override
    public void open() throws IOException {
        super.open();
        poller = new Zpoller(new long[] { socket.self });
    }

    @Override
    public void close() throws IOException {
        if (poller != null) {
            poller.close();
            poller = null;
        }
        super.close();
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
        //        final long socketPointer = poller.Wait(0);
        //        if (socketPointer != socket.self) {
        //            if (poller.expired()) {
        //                return null;
        //            } else if (poller.terminated()) {
        //                close();
        //                throw new EOFException("closed by other side");
        //            }
        //        }
        try (Zframe frame = Zframe.recv(socket.self)) {
            if (frame.self == 0) {
                throw new IOException("no frame");
            }
            final byte[] recv = frame.data();
            if (recv == null || recv.length == 0) {
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

}
