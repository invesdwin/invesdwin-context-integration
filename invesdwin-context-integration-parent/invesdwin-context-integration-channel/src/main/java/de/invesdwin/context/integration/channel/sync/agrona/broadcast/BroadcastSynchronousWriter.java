package de.invesdwin.context.integration.channel.sync.agrona.broadcast;

import java.io.IOException;
import java.io.InterruptedIOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class BroadcastSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    private static final int MESSAGE_TYPE_ID = 1;

    private final BroadcastTransmitter broadcastTransmitter;
    private IWriter writer;

    /**
     * Use fixedLength = null to disable zero copy
     */
    public BroadcastSynchronousWriter(final AtomicBuffer buffer) {
        this.broadcastTransmitter = new BroadcastTransmitter(buffer);
    }

    @Override
    public void open() throws IOException {
        this.writer = new ExpandableWriter(broadcastTransmitter);
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            try {
                writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
        }
        writer = null;
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        writer.write(message);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

    private static final class ExpandableWriter implements IWriter {
        private final BroadcastTransmitter broadcastTransmitter;
        private final IByteBuffer buffer;
        private final IByteBuffer messageBuffer;

        private ExpandableWriter(final BroadcastTransmitter broadcastTransmitter) {
            this.broadcastTransmitter = broadcastTransmitter;
            this.buffer = ByteBuffers.allocateExpandable();
            this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
        }

        @Override
        public void write(final IByteBufferProvider message) throws IOException {
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(SIZE_INDEX, size);
            send(MESSAGE_INDEX + size);
        }

        private void send(final int size) throws InterruptedIOException {
            broadcastTransmitter.transmit(MESSAGE_TYPE_ID, buffer.asDirectBuffer(), 0, size);
        }

    }

    private interface IWriter {
        void write(IByteBufferProvider message) throws IOException;
    }

}
