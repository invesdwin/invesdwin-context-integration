package de.invesdwin.context.integration.channel.sync.jnetrobust;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeArrayByteBuffer;

@NotThreadSafe
public class JnetrobustSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private final UnsafeArrayByteBuffer wrappedBuffer = new UnsafeArrayByteBuffer(Bytes.EMPTY_ARRAY);
    private IByteBuffer polledValue;
    private final JnetrobustSynchronousChannel channel;

    public JnetrobustSynchronousReader(final JnetrobustSynchronousChannel channel) {
        this.channel = channel;
        channel.registerReader();
    }

    @Override
    public void open() throws IOException {
        channel.open();
    }

    @Override
    public void close() throws IOException {
        channel.close();
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
        final byte[] receive;
        try {
            receive = channel.getProtocolHandle().receive();
            if (!channel.isWriterRegistered()) {
                //send acks
                channel.getProtocolHandle().send();
            }
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
        if (receive == null) {
            return null;
        }
        wrappedBuffer.wrap(receive);
        return wrappedBuffer;

    }

}
