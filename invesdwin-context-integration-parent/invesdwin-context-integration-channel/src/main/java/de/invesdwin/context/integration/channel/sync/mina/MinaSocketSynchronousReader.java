package de.invesdwin.context.integration.channel.sync.mina;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.buffer.SimpleBufferAllocator;
import org.apache.mina.core.filterchain.IoFilterAdapter;
import org.apache.mina.core.filterchain.IoFilterChain;
import org.apache.mina.core.session.IoSession;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@NotThreadSafe
public class MinaSocketSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private MinaSocketSynchronousChannel channel;
    private Reader reader;

    public MinaSocketSynchronousReader(final MinaSocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
    }

    @Override
    public void open() throws IOException {
        this.reader = new Reader(channel.getSocketSize());
        channel.open(channel -> {
            final IoFilterChain pipeline = channel.getFilterChain();
            pipeline.addLast("reader", reader);
        }, false);
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close();
            channel = null;
        }
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        final Reader readerCopy = reader;
        if (readerCopy == null) {
            throw FastEOFException.getInstance("already closed");
        }
        if (readerCopy.polledValue != null) {
            return true;
        }
        final IoBuffer polledValueBuf = readerCopy.polledValues.poll();
        if (polledValueBuf != null) {
            readerCopy.polledValueBuffer.wrap(polledValueBuf.buf());
            readerCopy.polledValueBuf = polledValueBuf;
            readerCopy.polledValue = readerCopy.polledValueBuffer.slice(0, polledValueBuf.limit());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final IByteBuffer value = reader.polledValue;
        reader.polledValue = null;
        if (ClosedByteBuffer.isClosed(value)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return value;
    }

    @Override
    public void readFinished() {
        if (reader.polledValueBuf != null) {
            reader.polledValueBuf.free();
            reader.polledValueBuf = null;
            reader.polledValueBuffer.wrap(ClosedByteBuffer.CLOSED_ARRAY);
        }
    }

    private final class Reader extends IoFilterAdapter implements Closeable {
        private final int socketSize;
        private final UnsafeByteBuffer buffer;
        private IoBuffer buf;
        private int targetPosition = MinaSocketSynchronousChannel.MESSAGE_INDEX;
        private int remaining = MinaSocketSynchronousChannel.MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;
        private final ManyToOneConcurrentLinkedQueue<IoBuffer> polledValues = new ManyToOneConcurrentLinkedQueue<>();
        private volatile IByteBuffer polledValue;
        private final UnsafeByteBuffer polledValueBuffer;
        private IoBuffer polledValueBuf;
        private boolean closed = false;

        private Reader(final int socketSize) {
            this.socketSize = socketSize;
            this.polledValueBuffer = new UnsafeByteBuffer();
            this.buffer = new UnsafeByteBuffer();
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
            }
            if (buf != null) {
                this.buf.free();
            }
            polledValue = ClosedByteBuffer.INSTANCE;
            if (polledValueBuf != null) {
                polledValueBuf.free();
                polledValueBuf = null;
                polledValueBuffer.wrap(ClosedByteBuffer.CLOSED_ARRAY);
            }
            IoBuffer polledValueBuf = polledValues.poll();
            while (polledValueBuf != null) {
                polledValueBuf.free();
                polledValueBuf = polledValues.poll();
            }
        }

        @Override
        public void exceptionCaught(final NextFilter nextFilter, final IoSession session, final Throwable cause)
                throws Exception {
            //connection must have been closed by the other side
            close();
            super.exceptionCaught(nextFilter, session, cause);
        }

        @Override
        public void messageReceived(final NextFilter nextFilter, final IoSession session, final Object message)
                throws Exception {
            final IoBuffer msgBuf = (IoBuffer) message;
            //CHECKSTYLE:OFF
            while (read(session, msgBuf)) {
            }
            //CHECKSTYLE:ON
            msgBuf.free();
        }

        private boolean read(final IoSession session, final IoBuffer msgBuf) {
            final int readable = msgBuf.remaining();
            final int read;
            final boolean repeat;
            if (readable > remaining) {
                read = remaining;
                repeat = true;
            } else {
                read = readable;
                repeat = false;
            }
            if (buf == null) {
                buf = new SimpleBufferAllocator().allocate(socketSize, true);
                buffer.wrap(buf.buf());
            }
            final int oldLimit = msgBuf.limit();
            msgBuf.limit(msgBuf.position() + read);
            buf.position(position);
            buf.put(msgBuf);
            buf.clear();
            msgBuf.limit(oldLimit);
            remaining -= read;
            position += read;

            if (position < targetPosition) {
                //we are still waiting for size of message to complete
                return repeat;
            }
            if (size == -1) {
                //read size and adjust target and remaining
                size = buffer.getInt(MinaSocketSynchronousChannel.SIZE_INDEX);
                if (size <= 0) {
                    polledValue = ClosedByteBuffer.INSTANCE;
                    return false;
                }
                targetPosition = size;
                remaining = size;
                position = 0;
                if (targetPosition > buffer.capacity()) {
                    //expand buffer to message size
                    buffer.ensureCapacity(targetPosition);
                }
                return repeat;
            }
            //message complete
            if (ClosedByteBuffer.isClosed((IByteBuffer) buffer, MinaSocketSynchronousChannel.SIZE_INDEX, size)) {
                polledValue = ClosedByteBuffer.INSTANCE;
                return false;
            } else {
                buf.position(0);
                buf.limit(size);
                polledValues.add(buf);
                buf = null;
                buffer.wrap(ClosedByteBuffer.CLOSED_ARRAY);
            }
            targetPosition = MinaSocketSynchronousChannel.MESSAGE_INDEX;
            remaining = MinaSocketSynchronousChannel.MESSAGE_INDEX;
            position = 0;
            size = -1;
            return repeat;
        }
    }

}
