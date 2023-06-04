package de.invesdwin.context.integration.channel.async.darpc;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;

@NotThreadSafe
public class DarpcServerAsynchronousChannel implements IAsynchronousChannel {

    @Override
    public void close() throws IOException {}

    @Override
    public void open() throws IOException {}

    @Override
    public boolean isClosed() {
        return false;
    }

    //    private DarpcServerSynchronousChannel channel;
    //    private final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler;
    //    private Reader reader;
    //
    //    public DarpcServerAsynchronousChannel(final DarpcServerSynchronousChannel channel,
    //            final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler) {
    //        channel.setReaderRegistered();
    //        channel.setWriterRegistered();
    //        this.channel = channel;
    //        this.handler = handler;
    //    }
    //
    //    @Override
    //    public void open() throws IOException {
    //        this.reader = new Reader(handler, channel.getSocketSize());
    //        channel.open(channel -> {
    //            final IoFilterChain pipeline = channel.getFilterChain();
    //            pipeline.addLast("reader", reader);
    //            if (!channel.isServer()) {
    //                try {
    //                    //need to call this manually for clients
    //                    reader.sessionOpened(null, channel);
    //                } catch (final Exception e) {
    //                    throw new RuntimeException(e);
    //                }
    //            }
    //        }, false);
    //    }
    //
    //    @Override
    //    public void close() {
    //        if (channel != null) {
    //            channel.close();
    //            channel = null;
    //        }
    //        if (reader != null) {
    //            reader.close();
    //            reader = null;
    //        }
    //        try {
    //            handler.close();
    //        } catch (final IOException e) {
    //            //ignore
    //        }
    //    }
    //
    //    public void closeAsync() {
    //        if (channel != null) {
    //            channel.closeAsync();
    //            channel = null;
    //        }
    //        if (reader != null) {
    //            reader.close();
    //            reader = null;
    //        }
    //        try {
    //            handler.close();
    //        } catch (final IOException e) {
    //            //ignore
    //        }
    //    }
    //
    //    @Override
    //    public boolean isClosed() {
    //        return channel == null || channel.getEndpoint() == null;
    //    }
    //
    //    private final class Reader implements DaRPCService<RdmaRpcMessage, RdmaRpcMessage>, Closeable {
    //        private final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler;
    //        private final IoBuffer buf;
    //        private final UnsafeByteBuffer buffer;
    //        private final IByteBuffer messageBuffer;
    //        private int targetPosition = MinaSocketSynchronousChannel.MESSAGE_INDEX;
    //        private int remaining = MinaSocketSynchronousChannel.MESSAGE_INDEX;
    //        private int position = 0;
    //        private int size = -1;
    //        private boolean closed = false;
    //
    //        private Reader(final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler, final int socketSize) {
    //            this.handler = handler;
    //            //netty uses direct buffers per default
    //            this.buf = new SimpleBufferAllocator().allocate(socketSize, true);
    //            this.buffer = new UnsafeByteBuffer(buf.buf());
    //            this.messageBuffer = buffer.newSliceFrom(MinaSocketSynchronousChannel.MESSAGE_INDEX);
    //        }
    //
    //        @Override
    //        public void processServerEvent(final DaRPCServerEvent<RdmaRpcMessage, RdmaRpcMessage> event)
    //                throws IOException {}
    //
    //        @Override
    //        public void open(final DaRPCServerEndpoint<RdmaRpcMessage, RdmaRpcMessage> rpcClientEndpoint) {}
    //
    //        @Override
    //        public void close(final DaRPCServerEndpoint<RdmaRpcMessage, RdmaRpcMessage> rpcClientEndpoint) {}
    //
    //        @Override
    //        public void close() {
    //            if (!closed) {
    //                closed = true;
    //                this.buf.free();
    //                closeAsync();
    //            }
    //        }
    //
    //        @Override
    //        public void exceptionCaught(final NextFilter nextFilter, final IoSession session, final Throwable cause)
    //                throws Exception {
    //            //connection must have been closed by the other side
    //            close();
    //            super.exceptionCaught(nextFilter, session, cause);
    //        }
    //
    //        @Override
    //        public void sessionOpened(final NextFilter nextFilter, final IoSession session) throws Exception {
    //            try {
    //                final IByteBufferProvider output = handler.open();
    //                writeOutput(session, output);
    //            } catch (final IOException e) {
    //                close();
    //            }
    //        }
    //
    //        @Override
    //        public void messageReceived(final NextFilter nextFilter, final IoSession session, final Object message)
    //                throws Exception {
    //            final IoBuffer msgBuf = (IoBuffer) message;
    //            //CHECKSTYLE:OFF
    //            while (read(session, msgBuf)) {
    //            }
    //            //CHECKSTYLE:ON
    //            msgBuf.free();
    //        }
    //
    //        private boolean read(final IoSession session, final IoBuffer msgBuf) {
    //            final int readable = msgBuf.remaining();
    //            final int read;
    //            final boolean repeat;
    //            if (readable > remaining) {
    //                read = remaining;
    //                repeat = true;
    //            } else {
    //                read = readable;
    //                repeat = false;
    //            }
    //            final int oldLimit = msgBuf.limit();
    //            msgBuf.limit(msgBuf.position() + read);
    //            buf.put(msgBuf);
    //            buf.clear();
    //            msgBuf.limit(oldLimit);
    //            remaining -= read;
    //            position += read;
    //
    //            if (position < targetPosition) {
    //                //we are still waiting for size of message to complete
    //                return repeat;
    //            }
    //            if (size == -1) {
    //                //read size and adjust target and remaining
    //                size = buffer.getInt(MinaSocketSynchronousChannel.SIZE_INDEX);
    //                if (size <= 0) {
    //                    DarpcServerAsynchronousChannel.this.closeAsync();
    //                    return false;
    //                }
    //                targetPosition = size;
    //                remaining = size;
    //                position = 0;
    //                if (targetPosition > buffer.capacity()) {
    //                    //expand buffer to message size
    //                    buffer.ensureCapacity(targetPosition);
    //                }
    //                return repeat;
    //            }
    //            //message complete
    //            if (ClosedByteBuffer.isClosed((IByteBuffer) buffer, 0, size)) {
    //                DarpcServerAsynchronousChannel.this.closeAsync();
    //                return false;
    //            } else {
    //                final IByteBuffer input = buffer.slice(0, size);
    //                try {
    //                    reset();
    //                    final IByteBufferProvider output = handler.handle(input);
    //                    writeOutput(session, output);
    //                    return repeat;
    //                } catch (final IOException e) {
    //                    try {
    //                        writeOutput(session, ClosedByteBuffer.INSTANCE);
    //                    } catch (final IOException e1) {
    //                        //ignore
    //                    }
    //                    DarpcServerAsynchronousChannel.this.closeAsync();
    //                    return false;
    //                }
    //            }
    //        }
    //
    //        private void reset() {
    //            targetPosition = MinaSocketSynchronousChannel.MESSAGE_INDEX;
    //            remaining = MinaSocketSynchronousChannel.MESSAGE_INDEX;
    //            position = 0;
    //            size = -1;
    //        }
    //
    //        private void writeOutput(final IoSession session, final IByteBufferProvider output) throws IOException {
    //            if (output != null) {
    //                buf.position(0); //reset indexes
    //                final int size = output.getBuffer(messageBuffer);
    //                buffer.putInt(MinaSocketSynchronousChannel.SIZE_INDEX, size);
    //                buf.limit(MinaSocketSynchronousChannel.MESSAGE_INDEX + size);
    //                session.write(buf);
    //            }
    //        }
    //    }

}
