package de.invesdwin.context.integration.channel.async.mina;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.buffer.SimpleBufferAllocator;
import org.apache.mina.core.filterchain.IoFilterAdapter;
import org.apache.mina.core.filterchain.IoFilterChain;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@NotThreadSafe
public class MinaSocketAsynchronousChannel implements IAsynchronousChannel {

    private MinaSocketSynchronousChannel channel;
    private final IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> handlerFactory;

    public MinaSocketAsynchronousChannel(final MinaSocketSynchronousChannel channel,
            final IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> handlerFactory,
            final boolean multipleClientsAllowed) {
        channel.setReaderRegistered();
        channel.setWriterRegistered();
        if (channel.isServer() && multipleClientsAllowed) {
            channel.setMultipleClientsAllowed();
        }
        this.channel = channel;
        this.handlerFactory = handlerFactory;
    }

    @Override
    public void open() throws IOException {
        channel.open(ch -> {
            final IoFilterChain pipeline = ch.getFilterChain();
            final Runnable closeAsync;
            if (channel.isMultipleClientsAllowed()) {
                closeAsync = () -> {
                };
            } else {
                closeAsync = MinaSocketAsynchronousChannel.this::closeAsync;
            }
            final Reader reader = new Reader(handlerFactory.newHandler(ch.toString()), channel.getSocketSize(),
                    closeAsync);
            pipeline.addLast("reader", reader);
            if (!ch.isServer()) {
                try {
                    //need to call this manually for clients
                    reader.sessionOpened(null, ch);
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
        try {
            handlerFactory.close();
        } catch (final IOException e) {
            //ignore
        }
    }

    public void closeAsync() {
        if (channel != null) {
            channel.closeAsync();
            channel = null;
        }
        try {
            handlerFactory.close();
        } catch (final IOException e) {
            //ignore
        }
    }

    @Override
    public boolean isClosed() {
        return channel == null || channel.isClosed();
    }

    private static final class Reader extends IoFilterAdapter {
        private final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler;
        private final IoBuffer buf;
        private final UnsafeByteBuffer buffer;
        private final IByteBuffer messageBuffer;
        private final Runnable closeAsync;
        private int targetPosition = MinaSocketSynchronousChannel.MESSAGE_INDEX;
        private int remaining = MinaSocketSynchronousChannel.MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;
        private boolean closed = false;

        private Reader(final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler,
                final int socketSize, final Runnable closeAsync) {
            this.handler = handler;
            //netty uses direct buffers per default
            this.buf = new SimpleBufferAllocator().allocate(socketSize, true);
            this.buffer = new UnsafeByteBuffer(buf.buf());
            this.messageBuffer = buffer.newSliceFrom(MinaSocketSynchronousChannel.MESSAGE_INDEX);
            this.closeAsync = closeAsync;
        }

        private void close(final IoSession session) {
            if (!closed) {
                closed = true;
                this.buf.free();
                session.closeNow();
                closeAsync.run();
            }
        }

        @Override
        public void exceptionCaught(final NextFilter nextFilter, final IoSession session, final Throwable cause)
                throws Exception {
            //connection must have been closed by the other side
            close(session);
            super.exceptionCaught(nextFilter, session, cause);
        }

        @Override
        public void sessionOpened(final NextFilter nextFilter, final IoSession session) throws Exception {
            try {
                final IByteBufferProvider output = handler.open();
                try {
                    writeOutput(session, output);
                } finally {
                    handler.outputFinished();
                }
            } catch (final IOException e) {
                close(session);
            }
        }

        @Override
        public void sessionClosed(final NextFilter nextFilter, final IoSession session) throws Exception {
            close(session);
        }

        @Override
        public void sessionIdle(final NextFilter nextFilter, final IoSession session, final IdleStatus status)
                throws Exception {
            try {
                final IByteBufferProvider output = handler.idle();
                try {
                    writeOutput(session, output);
                } finally {
                    handler.outputFinished();
                }
            } catch (final IOException e) {
                try {
                    writeOutput(session, ClosedByteBuffer.INSTANCE);
                } catch (final IOException e1) {
                    //ignore
                }
                close(session);
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
                size = buffer.getInt(MinaSocketSynchronousChannel.SIZE_INDEX);
                if (size <= 0) {
                    close(session);
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
                close(session);
                return false;
            } else {
                final IByteBuffer input = buffer.slice(0, size);
                try {
                    reset();
                    final IByteBufferProvider output = handler.handle(input);
                    try {
                        writeOutput(session, output);
                    } finally {
                        handler.outputFinished();
                    }
                    return repeat;
                } catch (final IOException e) {
                    try {
                        writeOutput(session, ClosedByteBuffer.INSTANCE);
                    } catch (final IOException e1) {
                        //ignore
                    }
                    close(session);
                    return false;
                }
            }
        }

        private void reset() {
            targetPosition = MinaSocketSynchronousChannel.MESSAGE_INDEX;
            remaining = MinaSocketSynchronousChannel.MESSAGE_INDEX;
            position = 0;
            size = -1;
        }

        private void writeOutput(final IoSession session, final IByteBufferProvider output) throws IOException {
            if (output != null) {
                buf.position(0); //reset indexes
                final int size = output.getBuffer(messageBuffer);
                buffer.putInt(MinaSocketSynchronousChannel.SIZE_INDEX, size);
                buf.limit(MinaSocketSynchronousChannel.MESSAGE_INDEX + size);
                session.write(buf);
            }
        }
    }

}
