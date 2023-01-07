package de.invesdwin.context.integration.channel.async.mina;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.buffer.SimpleBufferAllocator;
import org.apache.mina.core.filterchain.IoFilterAdapter;
import org.apache.mina.core.filterchain.IoFilterChain;
import org.apache.mina.core.session.IoSession;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@NotThreadSafe
public class MinaSocketAsynchronousChannel implements IAsynchronousChannel {

    private MinaSocketSynchronousChannel channel;
    private final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler;
    private Reader reader;

    public MinaSocketAsynchronousChannel(final MinaSocketSynchronousChannel channel,
            final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler) {
        channel.setReaderRegistered();
        channel.setWriterRegistered();
        this.channel = channel;
        this.handler = handler;
    }

    @Override
    public void open() throws IOException {
        this.reader = new Reader(handler, channel.getSocketSize());
        channel.open(channel -> {
            final IoFilterChain pipeline = channel.getFilterChain();
            pipeline.addLast("reader", reader);
            if (!channel.isServer()) {
                try {
                    //need to call this manually for clients
                    reader.sessionOpened(null, channel);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            }
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
        try {
            handler.close();
        } catch (final IOException e) {
            //ignore
        }
    }

    public void closeAsync() {
        if (channel != null) {
            channel.closeAsync();
            channel = null;
        }
        if (reader != null) {
            reader.close();
            reader = null;
        }
        try {
            handler.close();
        } catch (final IOException e) {
            //ignore
        }
    }

    @Override
    public boolean isClosed() {
        return channel == null || channel.getIoSession() == null;
    }

    private final class Reader extends IoFilterAdapter implements Closeable {
        private final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler;
        private final IoBuffer buf;
        private final UnsafeByteBuffer buffer;
        private final IByteBuffer messageBuffer;
        private int targetPosition = NettySocketSynchronousChannel.MESSAGE_INDEX;
        private int remaining = NettySocketSynchronousChannel.MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;
        private boolean closed = false;

        private Reader(final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler, final int socketSize) {
            this.handler = handler;
            //netty uses direct buffers per default
            this.buf = new SimpleBufferAllocator().allocate(socketSize, true);
            this.buffer = new UnsafeByteBuffer(buf.buf());
            this.messageBuffer = buffer.newSliceFrom(NettySocketSynchronousChannel.MESSAGE_INDEX);
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                this.buf.free();
                closeAsync();
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
        public void sessionOpened(final NextFilter nextFilter, final IoSession session) throws Exception {
            try {
                final IByteBufferProvider output = handler.open();
                writeOutput(session, output);
            } catch (final IOException e) {
                close();
            }
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
            final int oldLimit = msgBuf.limit();
            msgBuf.limit(msgBuf.position() + read);
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
                size = buffer.getInt(NettySocketSynchronousChannel.SIZE_INDEX);
                if (size <= 0) {
                    MinaSocketAsynchronousChannel.this.closeAsync();
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
            if (ClosedByteBuffer.isClosed((IByteBuffer) buffer, 0, size)) {
                MinaSocketAsynchronousChannel.this.closeAsync();
                return false;
            } else {
                final IByteBuffer input = buffer.slice(0, size);
                try {
                    reset();
                    final IByteBufferProvider output = handler.handle(input);
                    writeOutput(session, output);
                    return repeat;
                } catch (final IOException e) {
                    try {
                        writeOutput(session, ClosedByteBuffer.INSTANCE);
                    } catch (final IOException e1) {
                        //ignore
                    }
                    MinaSocketAsynchronousChannel.this.closeAsync();
                    return false;
                }
            }
        }

        private void reset() {
            targetPosition = NettySocketSynchronousChannel.MESSAGE_INDEX;
            remaining = NettySocketSynchronousChannel.MESSAGE_INDEX;
            position = 0;
            size = -1;
        }

        private void writeOutput(final IoSession session, final IByteBufferProvider output) throws IOException {
            if (output != null) {
                buf.position(0); //reset indexes
                final int size = output.getBuffer(messageBuffer);
                buffer.putInt(NettySocketSynchronousChannel.SIZE_INDEX, size);
                buf.limit(NettySocketSynchronousChannel.MESSAGE_INDEX + size);
                session.write(buf);
            }
        }
    }

}
