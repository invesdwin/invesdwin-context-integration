package de.invesdwin.context.integration.channel.agrona.broadcast;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.AgronaDelegateByteBuffer;

@NotThreadSafe
public class BroadcastSynchronousReader implements ISynchronousReader<IByteBuffer> {

    public static final int SIZE_INDEX = BroadcastSynchronousWriter.SIZE_INDEX;
    public static final int SIZE_SIZE = BroadcastSynchronousWriter.SIZE_SIZE;

    public static final int MESSAGE_INDEX = BroadcastSynchronousWriter.MESSAGE_INDEX;

    private final BroadcastReceiver broadcastReceiver;
    private final CopyBroadcastReceiver copyBroadcastReceiver;

    private IReader reader;

    public BroadcastSynchronousReader(final AtomicBuffer buffer) {
        this.broadcastReceiver = new BroadcastReceiver(buffer);
        this.copyBroadcastReceiver = new CopyBroadcastReceiver(broadcastReceiver, new ExpandableArrayBuffer());
    }

    @Override
    public void open() throws IOException {
        this.reader = new Reader();
    }

    @Override
    public void close() throws IOException {
        reader = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (reader.getPolledValue() != null) {
            return true;
        }
        copyBroadcastReceiver.receive(reader);
        return reader.getPolledValue() != null;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer message = getPolledMessage();
        if (message != null && ClosedByteBuffer.isClosed(message)) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

    private IByteBuffer getPolledMessage() {
        if (reader.getPolledValue() != null) {
            final IByteBuffer value = reader.getPolledValue();
            reader.close();
            return value;
        }
        try {
            final int messagesRead = copyBroadcastReceiver.receive(reader);
            if (messagesRead == 1) {
                final IByteBuffer value = reader.getPolledValue();
                reader.close();
                return value;
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private interface IReader extends MessageHandler, Closeable {
        IByteBuffer getPolledValue();

        @Override
        void close();

    }

    /**
     * We can use ZeroCopy here since CopyBroadcastReceiver already creates a safe copy.
     */
    private static final class Reader implements IReader {
        private final AgronaDelegateByteBuffer wrappedBuffer = new AgronaDelegateByteBuffer(
                AgronaDelegateByteBuffer.EMPTY_BYTES);

        private IByteBuffer polledValue;

        @Override
        public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index,
                final int length) {
            wrappedBuffer.setDelegate(buffer);
            final int size = wrappedBuffer.getInt(index + SIZE_INDEX);
            polledValue = wrappedBuffer.slice(index + MESSAGE_INDEX, size);
        }

        @Override
        public IByteBuffer getPolledValue() {
            return polledValue;
        }

        @Override
        public void close() {
            polledValue = null;
        }

    }

}
