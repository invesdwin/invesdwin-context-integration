package de.invesdwin.context.integration.channel.async.netty.udt;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udt.NettyUdtSynchronousChannel;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.udt.UdtMessage;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

@NotThreadSafe
public class NettyUdtAsynchronousChannel implements IAsynchronousChannel {

    private NettyUdtSynchronousChannel channel;
    private final IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> handlerFactory;

    public NettyUdtAsynchronousChannel(final NettyUdtSynchronousChannel channel,
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
        final Runnable closeAsync;
        if (channel.isMultipleClientsAllowed()) {
            closeAsync = () -> {
            };
        } else {
            closeAsync = NettyUdtAsynchronousChannel.this::closeAsync;
        }
        channel.open(bootstrap -> {
            bootstrap.handler(new Handler(ByteBufAllocator.DEFAULT, handlerFactory.newHandler(),
                    channel.getSocketSize(), closeAsync));
        }, ch -> {
            final ChannelPipeline pipeline = ch.pipeline();
            final Duration heartbeatInterval = handlerFactory.getHeartbeatInterval();
            final long heartbeatIntervalMillis = heartbeatInterval.longValue(FTimeUnit.MILLISECONDS);
            pipeline.addLast(new IdleStateHandler(heartbeatIntervalMillis, heartbeatIntervalMillis,
                    heartbeatIntervalMillis, TimeUnit.MILLISECONDS));
            pipeline.addLast(new Handler(ch.alloc(), handlerFactory.newHandler(), channel.getSocketSize(), closeAsync));
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

    private static final class Context implements IAsynchronousHandlerContext<IByteBufferProvider> {
        private static final AttributeKey<Context> CONTEXT_KEY = AttributeKey
                .newInstance(NettyUdtAsynchronousChannel.class.getSimpleName() + "_context");
        private final Channel ch;
        private final String sessionId;
        private final int socketSize;
        private AttributesMap attributes;

        private Context(final Channel ch, final int socketSize) {
            this.ch = ch;
            this.sessionId = ch.toString();
            this.socketSize = socketSize;
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
            ch.close();
        }

        @SuppressWarnings("deprecation")
        private void writeOutput(final IByteBufferProvider output) throws IOException {
            if (output != null) {
                final ByteBuf buf = ch.alloc().buffer(socketSize);
                final NettyDelegateByteBuffer buffer = new NettyDelegateByteBuffer(buf);
                final IByteBuffer messageBuffer = buffer.sliceFrom(NettySocketSynchronousChannel.MESSAGE_INDEX);
                final int size = output.getBuffer(messageBuffer);
                buffer.putInt(NettySocketSynchronousChannel.SIZE_INDEX, size);
                buf.setIndex(0, NettySocketSynchronousChannel.MESSAGE_INDEX + size);
                ch.writeAndFlush(new UdtMessage(buf));
            }
        }

        public static Context getOrCreate(final Channel ch, final int socketSize) {
            final Attribute<Context> attr = ch.attr(CONTEXT_KEY);
            final Context existing = attr.get();
            if (existing != null) {
                return existing;
            } else {
                final Context created = new Context(ch, socketSize);
                attr.set(created);
                return created;
            }
        }
    }

    private static final class Handler extends ChannelInboundHandlerAdapter {
        private final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler;
        private final int socketSize;
        private final ByteBuf buf;
        private final NettyDelegateByteBuffer buffer;
        private final IByteBuffer messageBuffer;
        private final Runnable closeAsync;
        private int targetPosition = NettySocketSynchronousChannel.MESSAGE_INDEX;
        private int remaining = NettySocketSynchronousChannel.MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;
        private boolean closed = false;

        private Handler(final ByteBufAllocator alloc,
                final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler, final int socketSize,
                final Runnable closeAsync) {
            this.handler = handler;
            this.socketSize = socketSize;
            //netty uses direct buffers per default
            this.buf = alloc.buffer(socketSize);
            this.buf.retain();
            this.buffer = new NettyDelegateByteBuffer(buf);
            this.messageBuffer = buffer.newSliceFrom(NettySocketSynchronousChannel.MESSAGE_INDEX);
            this.closeAsync = closeAsync;
        }

        private void close(final ChannelHandlerContext ctx) {
            if (!closed) {
                closed = true;
                this.buf.release();
                ctx.close();
                closeAsync.run();
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
            final Context context = Context.getOrCreate(ctx.channel(), socketSize);
            try {
                final IByteBufferProvider output = handler.open(context);
                try {
                    writeOutput(ctx, output);
                } finally {
                    handler.outputFinished(context);
                }
            } catch (final IOException e) {
                close(ctx);
            }
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
            close(ctx);
        }

        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                final Context context = Context.getOrCreate(ctx.channel(), socketSize);
                try {
                    final IByteBufferProvider output = handler.idle(context);
                    try {
                        writeOutput(ctx, output);
                    } finally {
                        handler.outputFinished(context);
                    }
                } catch (final IOException e) {
                    try {
                        writeOutput(ctx, ClosedByteBuffer.INSTANCE);
                    } catch (final IOException e1) {
                        //ignore
                    }
                    close(ctx);
                }
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

        @SuppressWarnings("deprecation")
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            final UdtMessage msgBuf = (UdtMessage) msg;
            //CHECKSTYLE:OFF
            while (read(ctx, msgBuf.content())) {
            }
            //CHECKSTYLE:ON
            msgBuf.release();
        }

        private boolean read(final ChannelHandlerContext ctx, final ByteBuf msgBuf) {
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
            msgBuf.readBytes(buf, position, read);
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
                    close(ctx);
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
            if (ClosedByteBuffer.isClosed(buffer, 0, size)) {
                close(ctx);
                return false;
            } else {
                final IByteBuffer input = buffer.slice(0, size);
                try {
                    reset();
                    final Context context = Context.getOrCreate(ctx.channel(), socketSize);
                    final IByteBufferProvider output = handler.handle(context, input);
                    try {
                        writeOutput(ctx, output);
                    } finally {
                        handler.outputFinished(context);
                    }
                    return repeat;
                } catch (final IOException e) {
                    try {
                        writeOutput(ctx, ClosedByteBuffer.INSTANCE);
                    } catch (final IOException e1) {
                        //ignore
                    }
                    close(ctx);
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

        @SuppressWarnings("deprecation")
        private void writeOutput(final ChannelHandlerContext ctx, final IByteBufferProvider output) throws IOException {
            if (output != null) {
                buf.setIndex(0, 0); //reset indexes
                final int size = output.getBuffer(messageBuffer);
                buffer.putInt(NettySocketSynchronousChannel.SIZE_INDEX, size);
                buf.setIndex(0, NettySocketSynchronousChannel.MESSAGE_INDEX + size);
                buf.retain();
                ctx.writeAndFlush(new UdtMessage(buf));
            }
        }
    }

}
