package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.io.IOException;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.FakeChannelPromise;
import de.invesdwin.context.integration.channel.sync.netty.FakeEventLoop;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.FastThreadLocal;

@NotThreadSafe
public class NettySocketSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    public static final FastThreadLocal<NettyDelegateByteBuffer> BUFFER_ALLOC_HOLDER = new FastThreadLocal<NettyDelegateByteBuffer>() {
        @Override
        protected NettyDelegateByteBuffer initialValue() throws Exception {
            return new NettyDelegateByteBuffer();
        }
    };

    private NettySocketSynchronousChannel channel;
    private ByteBuf buf;
    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private Consumer<ByteBuf> writer;
    private ChannelFuture future;
    private ByteBufAllocator bufAllocator;

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
                future = channel.getSocketChannel().writeAndFlush(message);
            };
        } else {
            channel.open(null);
            channel.getSocketChannel().deregister();
            channel.closeBootstrapAsync();
            FakeEventLoop.INSTANCE.register(channel.getSocketChannel());
            writer = (message) -> {
                channel.getSocketChannel().unsafe().write(message, FakeChannelPromise.INSTANCE);
                channel.getSocketChannel().unsafe().flush();
            };
        }
        this.buffer = new NettyDelegateByteBuffer(buf);
        this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettySocketSynchronousChannel.MESSAGE_INDEX);
        if (channel.isStreaming()) {
            this.bufAllocator = newStreamingByteBufAllocator(channel.getSocketChannel());
        } else {
            this.bufAllocator = null;
        }
    }

    /**
     * Return null to disable async writing, waiting for each message to be flushed before accepting another message.
     */
    protected ByteBufAllocator newStreamingByteBufAllocator(final SocketChannel socketChannel) {
        return socketChannel.alloc();
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
            bufAllocator = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        if (bufAllocator != null) {
            return true;
        }
        return isFutureDone();
    }

    private boolean isFutureDone() {
        if (future == null) {
            return true;
        }
        if (future.isDone()) {
            future = null;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        writeFuture(message);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

    private void writeFuture(final IByteBufferProvider message) throws IOException {
        if (isFutureDone()) {
            buf.setIndex(0, 0); //reset indexes
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(NettySocketSynchronousChannel.SIZE_INDEX, size);
            buf.setIndex(0, NettySocketSynchronousChannel.MESSAGE_INDEX + size);
            buf.retain(); //keep retain count up
            writer.accept(buf);
        } else {
            final ByteBuf bufAlloc = bufAllocator.directBuffer(channel.getSocketSize());
            bufAlloc.setIndex(0, 0); //reset indexes
            final NettyDelegateByteBuffer bufferAlloc = BUFFER_ALLOC_HOLDER.get();
            try {
                bufferAlloc.setDelegate(bufAlloc);
                final IByteBuffer messageBufferAlloc = bufferAlloc
                        .sliceFrom(NettySocketSynchronousChannel.MESSAGE_INDEX);
                final int size = message.getBuffer(messageBufferAlloc);
                bufferAlloc.putInt(NettySocketSynchronousChannel.SIZE_INDEX, size);
                bufAlloc.setIndex(0, NettySocketSynchronousChannel.MESSAGE_INDEX + size);
                channel.getSocketChannel().writeAndFlush(bufAlloc);
            } finally {
                bufferAlloc.setDelegate(null);
            }
        }
    }

}
