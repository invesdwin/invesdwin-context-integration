package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.FakeEventLoop;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

@NotThreadSafe
public class NettySocketSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

    private NettySocketChannel channel;
    private ByteBuf buf;
    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public NettySocketSynchronousWriter(final NettySocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open(null);
        channel.getSocketChannel().deregister();
        new FakeEventLoop().register(channel.getSocketChannel());
        //netty uses direct buffer per default
        this.buf = Unpooled.directBuffer(channel.getSocketSize());
        this.buf.retain();
        this.buffer = new NettyDelegateByteBuffer(buf);
        this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettySocketChannel.MESSAGE_INDEX);
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
        if (channel != null) {
            channel.close();
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
        channel.getSocketChannel().unsafe().write(buf, new ChannelPromise() {

            @Override
            public boolean trySuccess(final Void result) {
                return false;
            }

            @Override
            public boolean tryFailure(final Throwable cause) {
                return false;
            }

            @Override
            public boolean setUncancellable() {
                return false;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public Void get(final long timeout, final TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                return null;
            }

            @Override
            public Void get() throws InterruptedException, ExecutionException {
                return null;
            }

            @Override
            public boolean isSuccess() {
                return false;
            }

            @Override
            public boolean isCancellable() {
                return false;
            }

            @Override
            public Void getNow() {
                return null;
            }

            @Override
            public Throwable cause() {
                return null;
            }

            @Override
            public boolean cancel(final boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean awaitUninterruptibly(final long timeout, final TimeUnit unit) {
                return false;
            }

            @Override
            public boolean awaitUninterruptibly(final long timeoutMillis) {
                return false;
            }

            @Override
            public boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {
                return false;
            }

            @Override
            public boolean await(final long timeoutMillis) throws InterruptedException {
                return false;
            }

            @Override
            public boolean isVoid() {
                return false;
            }

            @Override
            public ChannelPromise unvoid() {
                return null;
            }

            @Override
            public boolean trySuccess() {
                return false;
            }

            @Override
            public ChannelPromise syncUninterruptibly() {
                return null;
            }

            @Override
            public ChannelPromise sync() throws InterruptedException {
                return null;
            }

            @Override
            public ChannelPromise setSuccess(final Void result) {
                return null;
            }

            @Override
            public ChannelPromise setSuccess() {
                return null;
            }

            @Override
            public ChannelPromise setFailure(final Throwable cause) {
                return null;
            }

            @Override
            public ChannelPromise removeListeners(
                    final GenericFutureListener<? extends Future<? super Void>>... listeners) {
                return null;
            }

            @Override
            public ChannelPromise removeListener(final GenericFutureListener<? extends Future<? super Void>> listener) {
                return null;
            }

            @Override
            public Channel channel() {
                return null;
            }

            @Override
            public ChannelPromise awaitUninterruptibly() {
                return null;
            }

            @Override
            public ChannelPromise await() throws InterruptedException {
                return null;
            }

            @Override
            public ChannelPromise addListeners(
                    final GenericFutureListener<? extends Future<? super Void>>... listeners) {
                return null;
            }

            @Override
            public ChannelPromise addListener(final GenericFutureListener<? extends Future<? super Void>> listener) {
                return null;
            }
        });
        channel.getSocketChannel().unsafe().flush();
    }

}
