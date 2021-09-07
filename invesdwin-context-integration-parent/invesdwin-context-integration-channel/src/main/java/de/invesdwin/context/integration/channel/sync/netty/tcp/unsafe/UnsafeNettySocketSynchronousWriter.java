package de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe;

import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.FakeChannelPromise;
import de.invesdwin.context.integration.channel.sync.netty.FakeEventLoop;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.SocketChannel;

@NotThreadSafe
public class UnsafeNettySocketSynchronousWriter extends NettySocketChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private ByteBuf buf;
    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public UnsafeNettySocketSynchronousWriter(final INettySocketChannelType type, final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open(null);
        socketChannel.deregister();
        new FakeEventLoop().register(socketChannel);
        //netty uses direct buffer per default
        this.buf = Unpooled.directBuffer(getSocketSize());
        this.buf.retain();
        this.buffer = new NettyDelegateByteBuffer(buf);
        this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettySocketChannel.MESSAGE_INDEX);
    }

    @Override
    protected void onSocketChannel(final SocketChannel socketChannel) {
        socketChannel.shutdownInput();
    }

    @Override
    public void close() {
        if (buffer != null) {
            writeFuture(ClosedByteBuffer.INSTANCE);
            buf.release();
            buf = null;
            buffer = null;
            messageBuffer = null;
        }
        super.close();
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
        socketChannel.unsafe().write(buf, FakeChannelPromise.INSTANCE);
        socketChannel.unsafe().flush();
    }

}
