package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.NettyDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.DatagramPacket;

@NotThreadSafe
public class NettyDatagramSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private NettyDatagramSynchronousChannel channel;
    private Reader reader;

    public NettyDatagramSynchronousReader(final NettyDatagramSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        this.channel.setKeepBootstrapRunningAfterOpen();
    }

    @Override
    public void open() throws IOException {
        this.reader = new Reader(channel);
        channel.open(bootstrap -> {
            bootstrap.handler(reader);
        }, channel -> {
            final ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(reader);
        });
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        final Reader readerCopy = reader;
        if (readerCopy == null) {
            throw FastEOFException.getInstance("already closed");
        }
        if (readerCopy.polledValue != null) {
            return true;
        }
        final ByteBuf polledValueBuf = readerCopy.polledValues.poll();
        if (polledValueBuf != null) {
            readerCopy.polledValueBuffer.setDelegate(polledValueBuf);
            readerCopy.polledValue = readerCopy.polledValueBuffer.slice(0, polledValueBuf.writerIndex());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final IByteBuffer value = reader.polledValue;
        reader.polledValue = null;
        if (ClosedByteBuffer.isClosed(value)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return value;
    }

    @Override
    public void readFinished() {
        final ByteBuf delegate = reader.polledValueBuffer.getDelegate();
        if (delegate != null) {
            delegate.release();
            reader.polledValueBuffer.setDelegate(null);
        }
    }

    private static final class Reader extends ChannelInboundHandlerAdapter implements Closeable {
        private final NettyDatagramSynchronousChannel channel;
        private final NettyDelegateByteBuffer buffer;
        private ByteBuf buf;
        private int targetPosition = NettyDatagramSynchronousChannel.MESSAGE_INDEX;
        private int remaining = NettyDatagramSynchronousChannel.MESSAGE_INDEX;
        private int position = 0;
        private int size = -1;
        private final ManyToOneConcurrentLinkedQueue<ByteBuf> polledValues = new ManyToOneConcurrentLinkedQueue<>();
        private volatile IByteBuffer polledValue;
        private final NettyDelegateByteBuffer polledValueBuffer;
        private boolean closed = false;

        private Reader(final NettyDatagramSynchronousChannel channel) {
            this.channel = channel;
            this.polledValueBuffer = new NettyDelegateByteBuffer();
            this.buffer = new NettyDelegateByteBuffer();
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
            }
            if (buf != null) {
                buf.release();
            }
            polledValue = ClosedByteBuffer.INSTANCE;
            final ByteBuf delegate = polledValueBuffer.getDelegate();
            if (delegate != null) {
                delegate.release();
                polledValueBuffer.setDelegate(null);
            }
            ByteBuf polledValueBuf = polledValues.poll();
            while (polledValueBuf != null) {
                polledValueBuf.release();
                polledValueBuf = polledValues.poll();
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
            //connection must have been closed by the other side
            close();
            super.exceptionCaught(ctx, cause);
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            final DatagramPacket msgBuf = (DatagramPacket) msg;
            if (channel.isMultipleClientsAllowed() || channel.getOtherSocketAddress() == null) {
                channel.setOtherSocketAddress(msgBuf.sender());
            }
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
            if (buf == null) {
                buf = ctx.alloc().buffer(channel.getSocketSize());
                buffer.setDelegate(buf);
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
                size = buffer.getInt(NettyDatagramSynchronousChannel.SIZE_INDEX);
                if (size <= 0) {
                    polledValue = ClosedByteBuffer.INSTANCE;
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
            if (ClosedByteBuffer.isClosed(buffer, NettyDatagramSynchronousChannel.SIZE_INDEX, size)) {
                polledValue = ClosedByteBuffer.INSTANCE;
                return false;
            } else {
                buf.readerIndex(0);
                buf.writerIndex(size);
                polledValues.add(buf);
                buf = null;
                buffer.setDelegate(null);
            }
            targetPosition = NettyDatagramSynchronousChannel.MESSAGE_INDEX;
            remaining = NettyDatagramSynchronousChannel.MESSAGE_INDEX;
            position = 0;
            size = -1;
            return repeat;
        }
    }

}
