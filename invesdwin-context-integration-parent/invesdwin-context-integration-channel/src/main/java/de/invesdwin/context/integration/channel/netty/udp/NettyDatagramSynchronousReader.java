package de.invesdwin.context.integration.channel.netty.udp;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.NettyDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

@NotThreadSafe
public class NettyDatagramSynchronousReader<M> extends ANettyDatagramSynchronousChannel
        implements ISynchronousReader<M> {

    private final ISerde<M> messageSerde;
    private Reader<M> reader;

    public NettyDatagramSynchronousReader(final INettyDatagramChannelType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize, final ISerde<M> messageSerde) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
        this.messageSerde = messageSerde;
    }

    @Override
    public void open() throws IOException {
        super.open();

        this.reader = new Reader<M>(messageSerde, socketSize);
        datagramChannel.pipeline().addLast(reader);
    }

    @Override
    public void close() {
        super.close();
        reader = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return reader.polledValue != null;
    }

    @Override
    public M readMessage() throws IOException {
        final M value = reader.polledValue.getAndSet(null);
        reader.polledValue = null;
        if (value == null) {
            close();
            throw new EOFException("closed by other side");
        }
        return value;
    }

    private static final class Reader<M> extends ChannelInboundHandlerAdapter {
        private final ISerde<M> messageSerde;
        private final ByteBuf buf;
        private final NettyDelegateByteBuffer buffer;
        private int targetPosition = MESSAGE_INDEX;
        private int remaining = MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;
        private final MutableReference<M> polledValueHolder = new MutableReference<>();
        private volatile IMutableReference<M> polledValue;

        private Reader(final ISerde<M> messageSerde, final int socketSize) {
            this.messageSerde = messageSerde;
            //netty uses direct buffers per default
            this.buf = Unpooled.directBuffer(socketSize);
            this.buffer = new NettyDelegateByteBuffer(buf);
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            final ByteBuf buf = (ByteBuf) msg;
            //CHECKSTYLE:OFF
            while (read(ctx, buf)) {
            }
            //CHECKSTYLE:ON
            buf.release();
        }

        private boolean read(final ChannelHandlerContext ctx, final ByteBuf buf) {
            final int readable = buf.readableBytes();
            final int read;
            final boolean repeat;
            if (readable > remaining) {
                read = remaining;
                repeat = true;
            } else {
                read = readable;
                repeat = false;
            }
            buf.readBytes(buf, position, read);
            remaining -= read;
            position += read;

            if (position < targetPosition) {
                //we are still waiting for size of message to complete
                return repeat;
            }
            if (size == -1) {
                //read size and adjust target and remaining
                size = buffer.getInt(SIZE_INDEX);
                targetPosition += size;
                remaining += size;
                if (targetPosition > buffer.capacity()) {
                    //expand buffer to message size
                    buffer.ensureCapacity(targetPosition);
                }
                return repeat;
            }
            //message complete
            if (ClosedByteBuffer.isClosed(buffer, MESSAGE_INDEX, size)) {
                polledValueHolder.set(null);
                polledValue = polledValueHolder;
            } else {
                final M value = messageSerde.fromBuffer(buffer, position);
                polledValueHolder.set(value);
                polledValue = polledValueHolder;
            }
            targetPosition = MESSAGE_INDEX;
            remaining = MESSAGE_INDEX;
            position = 0;
            size = -1;
            return repeat;
        }
    }

}
