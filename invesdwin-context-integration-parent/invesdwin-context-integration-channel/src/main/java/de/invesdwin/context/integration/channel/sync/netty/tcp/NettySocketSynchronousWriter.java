package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.io.IOException;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.FakeChannelPromise;
import de.invesdwin.context.integration.channel.sync.netty.FakeEventLoop;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;

@NotThreadSafe
public class NettySocketSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private NettySocketSynchronousChannel channel;
    private ByteBuf buf;
    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private Consumer<IByteBufferProvider> writer;

    public NettySocketSynchronousWriter(final NettySocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        //netty uses direct buffer per default
        this.buf = Unpooled.directBuffer(channel.getSocketSize());
        final boolean safeWriter = isSafeWriter(channel);
        if (safeWriter) {
            if (channel.isReaderRegistered()) {
                channel.open(null);
            } else {
                channel.open(channel -> {
                    channel.pipeline().addLast(new ChannelInboundHandlerAdapter());
                });
            }
            writer = (message) -> {
                channel.getSocketChannel().writeAndFlush(buf);
            };
        } else {
            channel.open(null);
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
        this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettySocketSynchronousChannel.MESSAGE_INDEX);
    }

    protected boolean isSafeWriter(final NettySocketSynchronousChannel channel) {
        //        final SocketChannel socketChannel = channel.getSocketChannel();
        //        return socketChannel instanceof IOUringSocketChannel
        //                || socketChannel instanceof io.netty.channel.socket.oio.OioSocketChannel
        //                || channel.isKeepBootstrapRunningAfterOpen();
        //unsafe interface will be removed in netty5, also unsafe makes tests flaky
        return true;
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
            channel.close();
            channel = null;
        }
    }

    @Override
    public void write(final IByteBufferProvider message) {
        writeFuture(message);
    }

    private void writeFuture(final IByteBufferProvider message) {
        buf.setIndex(0, 0); //reset indexes
        final int size = message.getBuffer(messageBuffer);
        buffer.putInt(NettySocketSynchronousChannel.SIZE_INDEX, size);
        buf.setIndex(0, NettySocketSynchronousChannel.MESSAGE_INDEX + size);
        buf.retain(); //keep retain count up
        writer.accept(message);
    }

}
