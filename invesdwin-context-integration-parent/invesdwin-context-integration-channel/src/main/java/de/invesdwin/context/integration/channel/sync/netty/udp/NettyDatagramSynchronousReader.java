package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.NettyDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.DatagramPacket;

@NotThreadSafe
public class NettyDatagramSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private NettyDatagramChannel channel;
    private Reader reader;

    public NettyDatagramSynchronousReader(final NettyDatagramChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        this.reader = new Reader(channel.getSocketSize());
        channel.open(channel -> {
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
    }

    @Override
    public boolean hasNext() throws IOException {
        return reader.polledValue != null;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer value = reader.polledValue;
        reader.polledValue = null;
        if (ClosedByteBuffer.isClosed(value)) {
            close();
            throw new EOFException("closed by other side");
        }
        return value;
    }

    private static final class Reader extends ChannelInboundHandlerAdapter implements Closeable {
        private final ByteBuf buf;
        private final NettyDelegateByteBuffer buffer;
        private int targetPosition = NettyDatagramChannel.MESSAGE_INDEX;
        private int remaining = NettyDatagramChannel.MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;
        private volatile IByteBuffer polledValue;

        private Reader(final int socketSize) {
            //netty uses direct buffers per default
            this.buf = Unpooled.directBuffer(socketSize);
            this.buf.retain();
            this.buffer = new NettyDelegateByteBuffer(buf);
        }

        @Override
        public void close() {
            buf.release();
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            final DatagramPacket msgBuf = (DatagramPacket) msg;
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
                size = buffer.getInt(NettyDatagramChannel.SIZE_INDEX);
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
            if (ClosedByteBuffer.isClosed(buffer, NettyDatagramChannel.SIZE_INDEX, size)) {
                polledValue = ClosedByteBuffer.INSTANCE;
            } else {
                polledValue = buffer.slice(0, size);
            }
            targetPosition = NettyDatagramChannel.MESSAGE_INDEX;
            remaining = NettyDatagramChannel.MESSAGE_INDEX;
            position = 0;
            size = -1;
            return repeat;
        }
    }

}