package de.invesdwin.context.integration.channel.async.mina;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.buffer.IoBufferAllocator;
import org.apache.mina.core.buffer.SimpleBufferAllocator;
import org.apache.mina.core.filterchain.IoFilterAdapter;
import org.apache.mina.core.filterchain.IoFilterChain;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.session.AttributeKey;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.async.netty.tcp.NettySocketAsynchronousChannel;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.util.collections.attributes.AttributesMap;
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
            final Reader reader = new Reader(handlerFactory.newHandler(), channel.getSocketSize(), closeAsync);
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

    private static final class Context implements IAsynchronousHandlerContext<IByteBufferProvider> {
        private static final AttributeKey CONTEXT_KEY = new AttributeKey(NettySocketAsynchronousChannel.class,
                "context");
        private final IoSession session;
        private final String sessionId;
        private final int socketSize;
        private final IoBufferAllocator alloc;
        private AttributesMap attributes;

        private Context(final IoSession session, final int socketSize, final IoBufferAllocator alloc) {
            this.session = session;
            this.sessionId = session.toString();
            this.socketSize = socketSize;
            this.alloc = alloc;
        }

        @Override
        public String getSessionId() {
            return sessionId;
        }

        @Override
        public AttributesMap getAttributes() {
            if (attributes == null) {
                synchronized (this) {
                    if (attributes == null) {
                        attributes = new AttributesMap();
                    }
                }
            }
            return attributes;
        }

        @Override
        public void write(final IByteBufferProvider output) {
            try {
                writeOutput(output);
            } catch (final IOException e) {
                close();
            }
        }

        @Override
        public void close() {
            try {
                writeOutput(ClosedByteBuffer.INSTANCE);
            } catch (final IOException e1) {
                //ignore
            }
            session.closeNow();
        }

        private void writeOutput(final IByteBufferProvider output) throws IOException {
            if (output != null) {
                writeOutputNotNullSafe(output);
            }
        }

        private void writeOutputNotNullSafe(final IByteBufferProvider output) throws IOException {
            final IoBuffer buf = alloc.allocate(socketSize, true);
            final UnsafeByteBuffer buffer = new UnsafeByteBuffer(buf.buf());
            final IByteBuffer messageBuffer = buffer.sliceFrom(MinaSocketSynchronousChannel.MESSAGE_INDEX);
            final int size = output.getBuffer(messageBuffer);
            buffer.putInt(NettySocketSynchronousChannel.SIZE_INDEX, size);
            buf.position(0);
            buf.limit(MinaSocketSynchronousChannel.MESSAGE_INDEX + size);
            session.write(buf);
        }

        public static Context getOrCreate(final IoSession ch, final int socketSize, final IoBufferAllocator alloc) {
            final Context existing = (Context) ch.getAttribute(CONTEXT_KEY);
            if (existing != null) {
                return existing;
            } else {
                final Context created = new Context(ch, socketSize, alloc);
                ch.setAttribute(CONTEXT_KEY, created);
                return created;
            }
        }
    }

    private static final class Reader extends IoFilterAdapter {
        private final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler;
        private final int socketSize;
        private final IoBufferAllocator alloc;
        private final IoBuffer buf;
        private final UnsafeByteBuffer buffer;
        private final IByteBuffer messageBuffer;
        private final Runnable closeAsync;
        private int targetPosition = MinaSocketSynchronousChannel.MESSAGE_INDEX;
        private int remaining = MinaSocketSynchronousChannel.MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;
        private boolean closed = false;
        private WriteFuture future;

        private Reader(final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler,
                final int socketSize, final Runnable closeAsync) {
            this.handler = handler;
            this.socketSize = socketSize;
            //netty uses direct buffers per default
            this.alloc = new SimpleBufferAllocator();
            this.buf = alloc.allocate(socketSize, true);
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
            final Context context = Context.getOrCreate(session, socketSize, alloc);
            try {
                final IByteBufferProvider output = handler.open(context);
                try {
                    writeOutput(session, context, output);
                } finally {
                    handler.outputFinished(context);
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
            final Context context = Context.getOrCreate(session, socketSize, alloc);
            try {
                final IByteBufferProvider output = handler.idle(context);
                try {
                    writeOutput(session, context, output);
                } finally {
                    handler.outputFinished(context);
                }
            } catch (final IOException e) {
                try {
                    writeOutput(session, context, ClosedByteBuffer.INSTANCE);
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
                final Context context = Context.getOrCreate(session, socketSize, alloc);
                try {
                    reset();
                    final IByteBufferProvider output = handler.handle(context, input);
                    try {
                        writeOutput(session, context, output);
                    } finally {
                        handler.outputFinished(context);
                    }
                    return repeat;
                } catch (final IOException e) {
                    try {
                        writeOutput(session, context, ClosedByteBuffer.INSTANCE);
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

        private void writeOutput(final IoSession session, final Context context, final IByteBufferProvider output)
                throws IOException {
            if (output != null) {
                if (future != null && !future.isDone()) {
                    //use a fresh buffer to not overwrite previous message
                    context.writeOutputNotNullSafe(output);
                } else {
                    //reuse buffer
                    buf.position(0); //reset indexes
                    final int size = output.getBuffer(messageBuffer);
                    buffer.putInt(MinaSocketSynchronousChannel.SIZE_INDEX, size);
                    buf.limit(MinaSocketSynchronousChannel.MESSAGE_INDEX + size);
                    future = session.write(buf);
                }
            }
        }
    }

}
