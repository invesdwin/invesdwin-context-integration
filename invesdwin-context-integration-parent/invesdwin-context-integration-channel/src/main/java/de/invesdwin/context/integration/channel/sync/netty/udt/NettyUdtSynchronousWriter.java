package de.invesdwin.context.integration.channel.sync.netty.udt;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.FakeChannelPromise;
import de.invesdwin.context.integration.channel.sync.netty.FakeEventLoop;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.INettyUdtChannelType;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.udt.UdtMessage;

@NotThreadSafe
public class NettyUdtSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    public static final boolean SERVER = false;
    private NettyUdtSynchronousChannel channel;
    private ByteBuf buf;
    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private UdtMessage udtMessage;
    private Runnable writer;

    public NettyUdtSynchronousWriter(final INettyUdtChannelType type, final InetSocketAddress socketAddress,
            final int estimatedMaxMessageSize) {
        this(new NettyUdtSynchronousChannel(type, socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public NettyUdtSynchronousWriter(final NettyUdtSynchronousChannel channel) {
        this.channel = channel;
        if (channel.isServer() != SERVER) {
            throw new IllegalStateException("udt writer has to be the client");
        }
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open(bootstrap -> {
            bootstrap.handler(new ChannelInboundHandlerAdapter());
        }, null);
        final boolean safeWriter = isSafeWriter(channel);
        if (safeWriter) {
            writer = () -> {
                channel.getUdtChannel().writeAndFlush(udtMessage);
            };
        } else {
            channel.getUdtChannel().deregister();
            channel.closeBootstrapAsync();
            FakeEventLoop.INSTANCE.register(channel.getUdtChannel());
            writer = () -> {
                channel.getUdtChannel().unsafe().write(udtMessage, FakeChannelPromise.INSTANCE);
                channel.getUdtChannel().unsafe().flush();
            };
        }
        //netty uses direct buffer per default
        this.buf = Unpooled.directBuffer(channel.getSocketSize());
        buf.retain();
        this.buffer = new NettyDelegateByteBuffer(buf);
        this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettyUdtSynchronousChannel.MESSAGE_INDEX);
        this.udtMessage = new UdtMessage(buf);
        this.udtMessage.retain();
    }

    protected boolean isSafeWriter(final NettyUdtSynchronousChannel channel) {
        //        final DatagramChannel udtChannel = channel.getDatagramChannel();
        //        return udtChannel instanceof io.netty.channel.socket.oio.OioDatagramChannel
        //                || udtChannel instanceof NioDatagramChannel || udtChannel instanceof IOUringDatagramChannel
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
            udtMessage.release();
            udtMessage = null;
            writer = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        writeFuture(message);
    }

    private void writeFuture(final IByteBufferProvider message) throws IOException {
        buf.setIndex(0, 0); //reset indexes
        final int size = message.getBuffer(messageBuffer);
        buffer.putInt(NettyUdtSynchronousChannel.SIZE_INDEX, size);
        buf.setIndex(0, NettyUdtSynchronousChannel.MESSAGE_INDEX + size);
        buf.retain(); //keep retain count up
        udtMessage.retain();
        writer.run();
    }

}