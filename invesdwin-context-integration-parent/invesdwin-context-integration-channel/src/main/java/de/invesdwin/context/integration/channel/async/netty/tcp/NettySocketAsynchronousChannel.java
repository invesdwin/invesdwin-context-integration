package de.invesdwin.context.integration.channel.async.netty.tcp;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.NettySocketChannel;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.NettyDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;

@NotThreadSafe
public class NettySocketAsynchronousChannel implements IAsynchronousChannel {

    private NettySocketChannel channel;
    private final IAsynchronousHandler<IByteBuffer, IByteBufferWriter> handler;
    private Reader reader;

    public NettySocketAsynchronousChannel(final NettySocketChannel channel,
            final IAsynchronousHandler<IByteBuffer, IByteBufferWriter> handler) {
        channel.setReaderRegistered();
        channel.setWriterRegistered();
        channel.setKeepBootstrapRunningAfterOpen();
        this.channel = channel;
        this.handler = handler;
    }

    @Override
    public void open() {
        this.reader = new Reader(handler, channel.getSocketSize());
        channel.open(channel -> {
            final ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(reader);
        });
    }

    @Override
    public void close() {
        if (channel != null) {
            try {
                channel.close();
            } catch (final IOException e) {
                //ignore
            }
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
        return channel == null;
    }

    private final class Reader extends ChannelInboundHandlerAdapter implements Closeable {
        private final IAsynchronousHandler<IByteBuffer, IByteBufferWriter> handler;
        private final ByteBuf buf;
        private final NettyDelegateByteBuffer buffer;
        private final IByteBuffer messageBuffer;
        private int targetPosition = NettySocketChannel.MESSAGE_INDEX;
        private int remaining = NettySocketChannel.MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;

        private Reader(final IAsynchronousHandler<IByteBuffer, IByteBufferWriter> handler, final int socketSize) {
            this.handler = handler;
            //netty uses direct buffers per default
            this.buf = Unpooled.directBuffer(socketSize);
            this.buf.retain();
            this.buffer = new NettyDelegateByteBuffer(buf);
            this.messageBuffer = buffer.newSliceFrom(NettySocketChannel.MESSAGE_INDEX);
        }

        @Override
        public void close() {
            this.buf.release();
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx) throws Exception {
            try {
                final IByteBufferWriter output = handler.open();
                writeOutput(ctx, output);
            } catch (final IOException e) {
                close();
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
                size = buffer.getInt(NettySocketChannel.SIZE_INDEX);
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
                NettySocketAsynchronousChannel.this.closeAsync();
            } else {
                final IByteBuffer input = buffer.slice(0, size);
                try {
                    final IByteBufferWriter output = handler.handle(input);
                    writeOutput(ctx, output);
                } catch (final IOException e) {
                    writeOutput(ctx, ClosedByteBuffer.INSTANCE);
                    NettySocketAsynchronousChannel.this.closeAsync();
                }
            }
            targetPosition = NettySocketChannel.MESSAGE_INDEX;
            remaining = NettySocketChannel.MESSAGE_INDEX;
            position = 0;
            size = -1;
            return repeat;
        }

        private void writeOutput(final ChannelHandlerContext ctx, final IByteBufferWriter output) {
            if (output != null) {
                final int size = output.writeBuffer(messageBuffer);
                buffer.putInt(NettySocketChannel.SIZE_INDEX, size);
                buf.setIndex(0, NettySocketChannel.MESSAGE_INDEX + size);
                buf.retain();
                ctx.writeAndFlush(buf);
            }
        }
    }

}
