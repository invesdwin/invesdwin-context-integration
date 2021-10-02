package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.FakeChannelPromise;
import de.invesdwin.context.integration.channel.sync.netty.FakeEventLoop;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.SocketChannel;
import io.netty.incubator.channel.uring.IOUringSocketChannel;

@NotThreadSafe
public class NettySocketSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

    private NettySocketChannel channel;
    private ByteBuf buf;
    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private Consumer<IByteBufferWriter> writer;

    public NettySocketSynchronousWriter(final INettySocketChannelType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        this(new NettySocketChannel(type, socketAddress, server, estimatedMaxMessageSize));
    }

    public NettySocketSynchronousWriter(final NettySocketChannel channel) {
        this.channel = channel;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void open() throws IOException {
        channel.open(null);
        //netty uses direct buffer per default
        this.buf = Unpooled.directBuffer(channel.getSocketSize());
        final boolean safeWriter = isSafeWriter(channel.getSocketChannel());
        if (safeWriter) {
            writer = (message) -> {
                channel.getSocketChannel().writeAndFlush(buf);
            };
        } else {
            channel.getSocketChannel().deregister();
            channel.closeBootstrapAsync();
            FakeEventLoop.INSTANCE.register(channel.getSocketChannel());
            writer = (message) -> {
                channel.getSocketChannel().unsafe().write(buf, FakeChannelPromise.INSTANCE);
                channel.getSocketChannel().unsafe().flush();
            };
        }
        this.buf.retain();
        this.buffer = new NettyDelegateByteBuffer(buf);
        this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettySocketChannel.MESSAGE_INDEX);
    }

    @SuppressWarnings("deprecation")
    protected boolean isSafeWriter(final SocketChannel socketChannel) {
        return socketChannel instanceof IOUringSocketChannel
                || socketChannel instanceof io.netty.channel.socket.oio.OioSocketChannel;
    }

    @Override
    public void close() {
        if (buffer != null) {
            try {
                writeFuture(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buf.release();
            buf = null;
            buffer = null;
            messageBuffer = null;
            writer = null;
        }
        if (channel != null) {
            try {
                channel.close();
            } catch (final IOException e) {
                //ignore
            }
            channel = null;
        }
    }

    @Override
    public void write(final IByteBufferWriter message) {
        writeFuture(message);
    }

    private void writeFuture(final IByteBufferWriter message) {
        final int size = message.write(messageBuffer);
        buffer.putInt(NettySocketChannel.SIZE_INDEX, size);
        buf.setIndex(0, NettySocketChannel.MESSAGE_INDEX + size);
        buf.retain(); //keep retain count up
        writer.accept(message);
    }

}
