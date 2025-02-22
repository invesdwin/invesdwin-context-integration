package de.invesdwin.context.integration.channel.async.netty.tcp;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResultPool;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.concurrent.future.NullFuture;
import de.invesdwin.util.concurrent.future.ThrowableFuture;
import de.invesdwin.util.lang.BroadcastingCloseable;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.string.Charsets;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.streams.buffer.bytes.delegate.ReusableByteBufInputStream;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

@NotThreadSafe
public class StringNettySocketAsynchronousChannel implements IAsynchronousChannel {

    public static final String CLOSED_MESSAGE = Strings.EMPTY;
    public static final char NEWLINE_CHAR = '\n';
    public static final String NEWLINE_STR = String.valueOf(NEWLINE_CHAR);

    private NettySocketSynchronousChannel channel;
    private final IAsynchronousHandlerFactory<String, String> handlerFactory;

    public StringNettySocketAsynchronousChannel(final NettySocketSynchronousChannel channel,
            final IAsynchronousHandlerFactory<String, String> handlerFactory, final boolean multipleClientsAllowed) {
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
        channel.open(ch -> {
            final ChannelPipeline pipeline = ch.pipeline();
            final Duration heartbeatInterval = handlerFactory.getHeartbeatInterval();
            final long heartbeatIntervalMillis = heartbeatInterval.longValue(FTimeUnit.MILLISECONDS);
            pipeline.addLast(new IdleStateHandler(heartbeatIntervalMillis, heartbeatIntervalMillis,
                    heartbeatIntervalMillis, TimeUnit.MILLISECONDS));
            final Runnable closeAsync;
            if (channel.isMultipleClientsAllowed()) {
                closeAsync = () -> {
                };
            } else {
                closeAsync = StringNettySocketAsynchronousChannel.this::closeAsync;
            }
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

    private static final class Context extends BroadcastingCloseable implements IAsynchronousHandlerContext<String> {

        private static final AttributeKey<Context> CONTEXT_KEY = AttributeKey
                .newInstance(StringNettySocketAsynchronousChannel.class.getSimpleName() + "_context");
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
        public Future<?> write(final String output) {
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
                writeOutput(CLOSED_MESSAGE);
            } catch (final IOException e1) {
                //ignore
            }
            ch.close();
        }

        private Future<?> writeOutput(final String output) throws IOException {
            if (output != null) {
                return writeOutputNotNullSafe(output);
            } else {
                return NullFuture.getInstance();
            }
        }

        private Future<?> writeOutputNotNullSafe(final String output) throws IOException {
            final ByteBuf buf = ch.alloc().buffer(socketSize);
            buf.writeCharSequence(output, Charsets.UTF_8);
            buf.writeCharSequence(NEWLINE_STR, Charsets.UTF_8);
            return ch.writeAndFlush(buf);
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
        public IAsynchronousHandlerContext<String> asImmutable() {
            return this;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(getSessionId()).toString();
        }
    }

    private static final class Handler extends ChannelInboundHandlerAdapter {

        private final IAsynchronousHandler<String, String> handler;
        private final int socketSize;
        private final ByteBuf inputBuf;
        private final ReusableByteBufInputStream inputBufStream;
        private final ByteBuf outputBuf;
        private final Runnable closeAsync;
        private boolean closed = false;
        private ChannelFuture future;

        private Handler(final ByteBufAllocator alloc, final IAsynchronousHandler<String, String> handler,
                final int socketSize, final Runnable closeAsync) {
            this.handler = handler;
            this.socketSize = socketSize;
            //netty uses direct buffers per default
            this.inputBuf = alloc.buffer(socketSize);
            this.inputBufStream = new ReusableByteBufInputStream();
            inputBufStream.wrap(inputBuf);
            this.outputBuf = alloc.buffer(socketSize);
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
            final Context context = Context.getOrCreate(ctx.channel(), socketSize);
            try {
                final String output = handler.open(context);
                if (output != null) {
                    try {
                        writeOutput(ctx, context, output);
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
            close(ctx);
        }

        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                final Context context = Context.getOrCreate(ctx.channel(), socketSize);
                try {
                    final String output = handler.idle(context);
                    if (output != null) {
                        try {
                            writeOutput(ctx, context, output);
                        } finally {
                            handler.outputFinished(context);
                        }
                    }
                } catch (final IOException e) {
                    try {
                        writeOutput(ctx, context, CLOSED_MESSAGE);
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
            final ByteBuf msgBuf = (ByteBuf) msg;
            //CHECKSTYLE:OFF
            while (read(ctx, msgBuf)) {
            }
            //CHECKSTYLE:ON
            msgBuf.release();
        }

        private boolean read(final ChannelHandlerContext ctx, final ByteBuf msgBuf) {
            final Context context = Context.getOrCreate(ctx.channel(), socketSize);
            try {
                final int readable = msgBuf.readableBytes();
                msgBuf.readBytes(inputBuf, 0, readable);
                inputBuf.writerIndex(inputBuf.writerIndex() + readable);
                final String input = inputBufStream.readLine();
                if (inputBuf.readerIndex() == inputBuf.writerIndex()) {
                    inputBuf.clear();
                }
                if (input == null) {
                    //message not complete
                    return false;
                }
                //message complete
                if (Strings.isBlank(input)) {
                    if (inputBufStream.lineBufLength() == input.length()) {
                        close(ctx);
                    }
                    return false;
                } else {
                    final String output = handler.handle(context, input);
                    if (output != null) {
                        try {
                            writeOutput(ctx, context, output);
                        } finally {
                            handler.outputFinished(context);
                        }
                    }
                    final boolean repeat = inputBufStream.available() > 0;
                    return repeat;
                }
            } catch (final IOException e) {
                try {
                    writeOutput(ctx, context, CLOSED_MESSAGE);
                } catch (final IOException e1) {
                    //ignore
                }
                close(ctx);
                return false;
            }
        }

        private void writeOutput(final ChannelOutboundInvoker ctx, final Context context, final String output)
                throws IOException {
            if (future != null && !future.isDone()) {
                //use a fresh buffer to not overwrite pending output message
                context.writeOutputNotNullSafe(output);
            } else {
                /*
                 * reuse buffer, though a separate output buffer so we don't accidentaly overwrite output with the next
                 * input during the same write cycle
                 */
                outputBuf.setIndex(0, 0); //reset indexes
                outputBuf.writeCharSequence(output, Charsets.UTF_8);
                outputBuf.writeCharSequence(NEWLINE_STR, Charsets.UTF_8);
                outputBuf.retain();
                future = ctx.writeAndFlush(outputBuf);
            }
        }
    }

}
