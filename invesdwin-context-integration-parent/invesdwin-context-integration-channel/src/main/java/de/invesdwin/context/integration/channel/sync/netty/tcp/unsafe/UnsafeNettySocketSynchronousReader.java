package de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.agrona.ringbuffer.RingBufferSynchronousReader;
import de.invesdwin.context.integration.channel.sync.agrona.ringbuffer.RingBufferSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.FakeEventLoop;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.NettyDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;

/**
 * Since netty reads in an asynchronous handler thread and the bytebuffer can/should not be shared with other threads,
 * the ISerde has to either copy the buffer or better directly convert it to the appropiate value type (for zero copy).
 */
@NotThreadSafe
public class UnsafeNettySocketSynchronousReader extends NettySocketChannel implements ISynchronousReader<IByteBuffer> {

    private Reader reader;

    public UnsafeNettySocketSynchronousReader(final INettySocketChannelType type, final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        this.reader = new Reader(getSocketSize());
        super.open(channel -> {
            channel.shutdownOutput();
            final ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(reader);
        });
        //        socketChannel.deregister();
        FakeEventLoop.INSTANCE.register(socketChannel);
    }

    @Override
    public void close() {
        super.close();
        if (reader != null) {
            try {
                reader.close();
            } catch (final IOException e) {
                //ignore
            }
            reader = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return reader.reader.hasNext();
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer value = reader.reader.readMessage();
        if (ClosedByteBuffer.isClosed(value)) {
            close();
            throw new EOFException("closed by other side");
        }
        return value;
    }

    private static final class Reader extends ChannelInboundHandlerAdapter implements Closeable {
        private final ByteBuf buf;
        private final NettyDelegateByteBuffer buffer;
        private int targetPosition = NettySocketChannel.MESSAGE_INDEX;
        private int remaining = NettySocketChannel.MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;
        private final RingBufferSynchronousReader reader;
        private final RingBufferSynchronousWriter writer;

        private Reader(final int socketSize) throws IOException {
            //netty uses direct buffers per default
            this.buf = Unpooled.directBuffer(socketSize);
            this.buf.retain();
            this.buffer = new NettyDelegateByteBuffer(buf);
            final int bufferSize = 4096 + RingBufferDescriptor.TRAILER_LENGTH;
            final RingBuffer ringBuffer = new OneToOneRingBuffer(
                    new UnsafeBuffer(java.nio.ByteBuffer.allocate(bufferSize)));
            this.reader = new RingBufferSynchronousReader(ringBuffer, true);
            this.writer = new RingBufferSynchronousWriter(ringBuffer, null);
            this.reader.open();
            this.writer.open();
        }

        @Override
        public void close() throws IOException {
            this.buf.release();
            this.reader.close();
            this.writer.close();
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            final ByteBuf msgBuf = (ByteBuf) msg;
            //CHECKSTYLE:OFF
            try {
                while (read(ctx, msgBuf)) {
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            //CHECKSTYLE:ON
            msgBuf.release();
        }

        private boolean read(final ChannelHandlerContext ctx, final ByteBuf msgBuf) throws IOException {
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
            if (ClosedByteBuffer.isClosed(buffer, NettySocketChannel.SIZE_INDEX, size)) {
                writer.write(ClosedByteBuffer.INSTANCE);
            } else {
                writer.write(buffer.slice(0, size));
            }
            targetPosition = NettySocketChannel.MESSAGE_INDEX;
            remaining = NettySocketChannel.MESSAGE_INDEX;
            position = 0;
            size = -1;
            return repeat;
        }
    }

}
