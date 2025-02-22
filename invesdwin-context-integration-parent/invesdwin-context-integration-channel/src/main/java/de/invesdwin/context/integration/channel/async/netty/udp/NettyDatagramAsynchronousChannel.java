package de.invesdwin.context.integration.channel.async.netty.udp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResultPool;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramSynchronousChannel;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.concurrent.future.NullFuture;
import de.invesdwin.util.concurrent.future.ThrowableFuture;
import de.invesdwin.util.lang.BroadcastingCloseable;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

@NotThreadSafe
public class NettyDatagramAsynchronousChannel implements IAsynchronousChannel {

    private NettyDatagramSynchronousChannel channel;
    private final IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> handlerFactory;

    public NettyDatagramAsynchronousChannel(final NettyDatagramSynchronousChannel channel,
            final IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> handlerFactory,
            final boolean multipleClientsAllowed) {
        channel.setReaderRegistered();
        channel.setWriterRegistered();
        channel.setKeepBootstrapRunningAfterOpen();
        if (channel.isServer() && multipleClientsAllowed) {
            channel.setMultipleClientsAllowed();
        }
        this.channel = channel;
        this.handlerFactory = handlerFactory;
    }

    @Override
    public void open() throws IOException {
        handlerFactory.open();
        final Runnable closeAsync;
        if (channel.isMultipleClientsAllowed()) {
            closeAsync = () -> {
            };
        } else {
            closeAsync = NettyDatagramAsynchronousChannel.this::closeAsync;
        }
        channel.open(bootstrap -> {
            final InetSocketAddress openRecipient;
            if (channel.isServer()) {
                openRecipient = null;
            } else {
                openRecipient = channel.getSocketAddress();
            }
            bootstrap.handler(new Handler(ByteBufAllocator.DEFAULT, handlerFactory.newHandler(), openRecipient, channel,
                    closeAsync));
        }, ch -> {
            final ChannelPipeline pipeline = ch.pipeline();
            final Duration heartbeatInterval = handlerFactory.getHeartbeatInterval();
            final long heartbeatIntervalMillis = heartbeatInterval.longValue(FTimeUnit.MILLISECONDS);
            pipeline.addLast(new IdleStateHandler(heartbeatIntervalMillis, heartbeatIntervalMillis,
                    heartbeatIntervalMillis, TimeUnit.MILLISECONDS));
            pipeline.addLast(
                    new Handler(ch.alloc(), handlerFactory.newHandler(), ch.remoteAddress(), channel, closeAsync));
        });
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

    private static final class Context extends BroadcastingCloseable
            implements IAsynchronousHandlerContext<IByteBufferProvider> {
        private static final AttributeKey<Context> CONTEXT_KEY = AttributeKey
                .newInstance(NettyDatagramAsynchronousChannel.class.getSimpleName() + "_context");
        private final DatagramChannel ch;
        private final String sessionId;
        private final int socketSize;
        private InetSocketAddress remoteAddressOverride;
        private AttributesMap attributes;

        private Context(final DatagramChannel ch, final int socketSize, final InetSocketAddress remoteAddressOverride) {
            this.ch = ch;
            this.sessionId = ch.toString();
            this.socketSize = socketSize;
            this.remoteAddressOverride = remoteAddressOverride;
        }

        public InetSocketAddress getRemoteAddressOverride() {
            if (remoteAddressOverride != null) {
                return remoteAddressOverride;
            } else {
                return ch.remoteAddress();
            }
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
        public Future<?> write(final IByteBufferProvider output) {
            try {
                return writeOutput(output);
            } catch (final IOException e) {
                close();
                return ThrowableFuture.of(e);
            }
        }

        @Override
        public void close() {
            super.close();
            try {
                writeOutput(ClosedByteBuffer.INSTANCE);
            } catch (final IOException e1) {
                //ignore
            }
            ch.close();
        }

        private Future<?> writeOutput(final IByteBufferProvider output) throws IOException {
            if (output != null) {
                return writeOutputNotNullSafe(output);
            } else {
                return NullFuture.getInstance();
            }
        }

        private Future<?> writeOutputNotNullSafe(final IByteBufferProvider output) throws IOException {
            final ByteBuf buf = ch.alloc().buffer(socketSize);
            final NettyDelegateByteBuffer buffer = new NettyDelegateByteBuffer(buf);
            final IByteBuffer messageBuffer = buffer.sliceFrom(NettyDatagramSynchronousChannel.MESSAGE_INDEX);
            final int size = output.getBuffer(messageBuffer);
            buffer.putInt(NettyDatagramSynchronousChannel.SIZE_INDEX, size);
            buf.setIndex(0, NettyDatagramSynchronousChannel.MESSAGE_INDEX + size);
            return ch.writeAndFlush(new DatagramPacket(buf, getRemoteAddressOverride()));
        }

        public static Context getOrCreate(final Channel ch, final int socketSize,
                final InetSocketAddress remoteAddressOverride) {
            final Attribute<Context> attr = ch.attr(CONTEXT_KEY);
            final Context existing = attr.get();
            if (existing != null) {
                existing.remoteAddressOverride = remoteAddressOverride;
                return existing;
            } else {
                final Context created = new Context((DatagramChannel) ch, socketSize, remoteAddressOverride);
                attr.set(created);
                return created;
            }
        }

        public static Context get(final Channel ch) {
            final Attribute<Context> attr = ch.attr(CONTEXT_KEY);
            return attr.get();
        }

        @Override
        public ProcessResponseResult borrowResult() {
            return ProcessResponseResultPool.INSTANCE.borrowObject();
        }

        @Override
        public void returnResult(final ProcessResponseResult result) {
            ProcessResponseResultPool.INSTANCE.returnObject(result);
        }

        @Override
        public IAsynchronousHandlerContext<IByteBufferProvider> asImmutable() {
            return this;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(getSessionId()).toString();
        }
    }

    private static final class Handler extends ChannelInboundHandlerAdapter {
        private final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler;
        private final NettyDatagramSynchronousChannel channel;
        private final InetSocketAddress openRecipient;
        private final ByteBuf inputBuf;
        private final NettyDelegateByteBuffer inputBuffer;
        private final ByteBuf outputBuf;
        private final NettyDelegateByteBuffer outputBuffer;
        private final IByteBuffer outputMessageBuffer;
        private final Runnable closeAsync;
        private int targetPosition = NettyDatagramSynchronousChannel.MESSAGE_INDEX;
        private int remaining = NettyDatagramSynchronousChannel.MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;
        private boolean closed = false;
        private ChannelFuture future;

        private Handler(final ByteBufAllocator alloc,
                final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler,
                final InetSocketAddress openRecipient, final NettyDatagramSynchronousChannel channel,
                final Runnable closeAsync) {
            this.handler = handler;
            this.channel = channel;
            this.openRecipient = openRecipient;
            //netty uses direct buffers per default
            this.inputBuf = alloc.buffer(channel.getSocketSize());
            this.inputBuffer = new NettyDelegateByteBuffer(inputBuf);
            this.outputBuf = alloc.buffer(channel.getSocketSize());
            this.outputBuffer = new NettyDelegateByteBuffer(outputBuf);
            this.outputMessageBuffer = outputBuffer.newSliceFrom(NettyDatagramSynchronousChannel.MESSAGE_INDEX);
            this.closeAsync = closeAsync;
        }

        private void close(final ChannelHandlerContext ctx) {
            if (!closed) {
                closed = true;
                final Context context = Context.get(ctx.channel());
                if (context != null) {
                    context.close();
                }
                this.inputBuf.release();
                this.outputBuf.release();
                ctx.close();
                closeAsync.run();
                future = null;
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
            //connection must have been closed by the other side
            close(ctx);
            super.exceptionCaught(ctx, cause);
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx) throws Exception {
            final InetSocketAddress recipient;
            if (openRecipient != null) {
                recipient = openRecipient;
            } else {
                recipient = (InetSocketAddress) ctx.channel().remoteAddress();
            }
            final Context context = Context.getOrCreate(ctx.channel(), channel.getSocketSize(), recipient);
            try {
                final IByteBufferProvider output = handler.open(context);
                if (output != null) {
                    try {
                        writeOutput(ctx, context, context.getRemoteAddressOverride(), output);
                    } finally {
                        handler.outputFinished(context);
                    }
                }
            } catch (final IOException e) {
                close(ctx);
            }
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
            if (!channel.isMultipleClientsAllowed()) {
                close(ctx);
            }
        }

        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                final InetSocketAddress recipient;
                if (openRecipient != null) {
                    recipient = openRecipient;
                } else {
                    recipient = (InetSocketAddress) ctx.channel().remoteAddress();
                }
                if (recipient == null) {
                    return;
                }
                final Context context = Context.getOrCreate(ctx.channel(), channel.getSocketSize(), recipient);
                try {
                    final IByteBufferProvider output = handler.idle(context);
                    if (output != null) {
                        try {
                            writeOutput(ctx, context, context.getRemoteAddressOverride(), output);
                        } finally {
                            handler.outputFinished(context);
                        }
                    }
                } catch (final IOException e) {
                    try {
                        writeOutput(ctx, context, context.getRemoteAddressOverride(), ClosedByteBuffer.INSTANCE);
                    } catch (final IOException e1) {
                        //ignore
                    }
                    close(ctx);
                }
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            final DatagramPacket msgBuf = (DatagramPacket) msg;
            //CHECKSTYLE:OFF
            while (read(ctx, msgBuf.sender(), msgBuf.content())) {
            }
            //CHECKSTYLE:ON
            msgBuf.release();
        }

        private boolean read(final ChannelHandlerContext ctx, final InetSocketAddress sender, final ByteBuf msgBuf) {
            final int readable = msgBuf.readableBytes();
            final int read;
            final boolean repeat;
            if (readable > remaining) {
                read = remaining;
                repeat = true;
            } else {
                read = readable;
                repeat = false;
            }
            msgBuf.readBytes(inputBuf, position, read);
            remaining -= read;
            position += read;

            if (position < targetPosition) {
                //we are still waiting for size of message to complete
                return repeat;
            }
            if (size == -1) {
                //read size and adjust target and remaining
                size = inputBuffer.getInt(NettyDatagramSynchronousChannel.SIZE_INDEX);
                if (size <= 0) {
                    close(ctx);
                    return false;
                }
                targetPosition = size;
                remaining = size;
                position = 0;
                if (targetPosition > inputBuffer.capacity()) {
                    //expand buffer to message size
                    inputBuffer.ensureCapacity(targetPosition);
                }
                return repeat;
            }
            //message complete
            if (ClosedByteBuffer.isClosed(inputBuffer, 0, size)) {
                if (!channel.isMultipleClientsAllowed()) {
                    close(ctx);
                }
                return false;
            } else {
                final IByteBuffer input = inputBuffer.slice(0, size);
                final Context context = Context.getOrCreate(ctx.channel(), channel.getSocketSize(), sender);
                try {
                    reset();
                    final IByteBufferProvider output = handler.handle(context, input);
                    if (output != null) {
                        try {
                            writeOutput(ctx, context, sender, output);
                        } finally {
                            handler.outputFinished(context);
                        }
                    }
                    return repeat;
                } catch (final IOException e) {
                    try {
                        writeOutput(ctx, context, sender, ClosedByteBuffer.INSTANCE);
                    } catch (final IOException e1) {
                        //ignore
                    }
                    close(ctx);
                    return false;
                }
            }
        }

        private void reset() {
            targetPosition = NettyDatagramSynchronousChannel.MESSAGE_INDEX;
            remaining = NettyDatagramSynchronousChannel.MESSAGE_INDEX;
            position = 0;
            size = -1;
        }

        private void writeOutput(final ChannelHandlerContext ctx, final Context context, final InetSocketAddress sender,
                final IByteBufferProvider output) throws IOException {
            if (future != null && !future.isDone()) {
                //use a fresh buffer to not overwrite pending output message
                context.writeOutputNotNullSafe(output);
            } else {
                /*
                 * reuse buffer, though a separate output buffer so we don't accidentaly overwrite output with the next
                 * input during the same write cycle
                 */
                outputBuf.setIndex(0, 0); //reset indexes
                final int size = output.getBuffer(outputMessageBuffer);
                outputBuffer.putInt(NettyDatagramSynchronousChannel.SIZE_INDEX, size);
                outputBuf.setIndex(0, NettyDatagramSynchronousChannel.MESSAGE_INDEX + size);
                outputBuf.retain();
                future = ctx.writeAndFlush(new DatagramPacket(outputBuf, sender));
            }
        }
    }

}
