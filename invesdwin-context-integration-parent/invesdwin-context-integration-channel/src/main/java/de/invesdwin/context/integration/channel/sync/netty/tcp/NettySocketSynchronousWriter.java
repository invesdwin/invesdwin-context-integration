package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.FakeChannelPromise;
import de.invesdwin.context.integration.channel.sync.netty.FakeEventLoop;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@NotThreadSafe
public class NettySocketSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

    private NettySocketChannel channel;
    private ByteBuf buf;
    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public NettySocketSynchronousWriter(final INettySocketChannelType type, final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        this(new NettySocketChannel(type, socketAddress, server, estimatedMaxMessageSize));
    }

    public NettySocketSynchronousWriter(final NettySocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open(null);
        //netty uses direct buffer per default
        this.buf = Unpooled.directBuffer(channel.getSocketSize());
        channel.getSocketChannel().deregister();
        FakeEventLoop.INSTANCE.register(channel.getSocketChannel());
        this.buf.retain();
        this.buffer = new NettyDelegateByteBuffer(buf);
        this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettySocketChannel.MESSAGE_INDEX);
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
        channel.getSocketChannel().unsafe().write(buf, FakeChannelPromise.INSTANCE);
        channel.getSocketChannel().unsafe().flush();
    }

}
