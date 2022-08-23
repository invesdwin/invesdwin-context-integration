package de.invesdwin.context.integration.channel.async.netty.udp;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.NettyDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.DatagramPacket;

@NotThreadSafe
public class NettyDatagramAsynchronousChannel implements IAsynchronousChannel {

    private NettyDatagramSynchronousChannel channel;
    private final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler;
    private Reader reader;

    public NettyDatagramAsynchronousChannel(final NettyDatagramSynchronousChannel channel,
            final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler) {
        channel.setReaderRegistered();
        channel.setWriterRegistered();
        channel.setKeepBootstrapRunningAfterOpen();
        this.channel = channel;
        this.handler = handler;
    }

    @Override
    public void open() {
        this.reader = new Reader(handler, channel.getSocketAddress(), channel.getSocketSize());
        channel.open(bootstrap -> {
            bootstrap.handler(reader);
        }, channel -> {
            final ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(reader);
        });
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
        return channel == null || channel.getDatagramChannel() == null;
    }

    private final class Reader extends ChannelInboundHandlerAdapter implements Closeable {
        private final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler;
        private final InetSocketAddress recipient;
        private final ByteBuf buf;
        private final NettyDelegateByteBuffer buffer;
        private final IByteBuffer messageBuffer;
        private int targetPosition = NettySocketSynchronousChannel.MESSAGE_INDEX;
        private int remaining = NettySocketSynchronousChannel.MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;
        private boolean closed = false;

        private Reader(final IAsynchronousHandler<IByteBuffer, IByteBufferProvider> handler,
                final InetSocketAddress recipient, final int socketSize) {
            this.handler = handler;
            this.recipient = recipient;
            //netty uses direct buffers per default
            this.buf = Unpooled.directBuffer(socketSize);
            this.buf.retain();
            this.buffer = new NettyDelegateByteBuffer(buf);
            this.messageBuffer = buffer.newSliceFrom(NettySocketSynchronousChannel.MESSAGE_INDEX);
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                this.buf.release();
                closeAsync();
            }
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx) throws Exception {
            try {
                final IByteBufferProvider output = handler.open();
                writeOutput(ctx, recipient, output);
            } catch (final IOException e) {
                close();
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
                NettyDatagramAsynchronousChannel.this.closeAsync();
            } else {
                final IByteBuffer input = buffer.slice(0, size);
                try {
                    final IByteBufferProvider output = handler.handle(input);
                    writeOutput(ctx, sender, output);
                } catch (final IOException e) {
                    writeOutput(ctx, sender, ClosedByteBuffer.INSTANCE);
                    NettyDatagramAsynchronousChannel.this.closeAsync();
                }
            }
            targetPosition = NettySocketSynchronousChannel.MESSAGE_INDEX;
            remaining = NettySocketSynchronousChannel.MESSAGE_INDEX;
            position = 0;
            size = -1;
            return repeat;
        }

        private void writeOutput(final ChannelHandlerContext ctx, final InetSocketAddress sender,
                final IByteBufferProvider output) {
            if (output != null) {
                buf.setIndex(0, 0); //reset indexes
                final int size = output.getBuffer(messageBuffer);
                buffer.putInt(NettySocketSynchronousChannel.SIZE_INDEX, size);
                buf.setIndex(0, NettySocketSynchronousChannel.MESSAGE_INDEX + size);
                buf.retain();
                ctx.writeAndFlush(new DatagramPacket(buf, sender));
            }
        }
    }

}
