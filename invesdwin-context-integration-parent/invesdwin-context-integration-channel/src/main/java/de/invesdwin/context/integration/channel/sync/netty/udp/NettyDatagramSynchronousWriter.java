package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.FakeChannelPromise;
import de.invesdwin.context.integration.channel.sync.netty.FakeEventLoop;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

@NotThreadSafe
public class NettyDatagramSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

    public static final boolean SERVER = false;
    private NettyDatagramChannel channel;
    private ByteBuf buf;
    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private DatagramPacket datagramPacket;
    private Runnable writer;

    public NettyDatagramSynchronousWriter(final INettyDatagramChannelType type, final InetSocketAddress socketAddress,
            final int estimatedMaxMessageSize) {
        this(new NettyDatagramChannel(type, socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public NettyDatagramSynchronousWriter(final NettyDatagramChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open(bootstrap -> {
            bootstrap.handler(new ChannelInboundHandlerAdapter());
        }, null);
        //netty uses direct buffer per default
        this.buf = Unpooled.directBuffer(channel.getSocketSize());
        final boolean nioWorkaround = channel.getDatagramChannel() instanceof NioDatagramChannel;
        if (nioWorkaround) {
            writer = () -> {
                channel.getDatagramChannel().writeAndFlush(datagramPacket);
            };
        } else {
            channel.getDatagramChannel().deregister();
            FakeEventLoop.INSTANCE.register(channel.getDatagramChannel());
            writer = () -> {
                channel.getDatagramChannel().unsafe().write(datagramPacket, FakeChannelPromise.INSTANCE);
                channel.getDatagramChannel().unsafe().flush();
            };
        }
        buf.retain();
        this.buffer = new NettyDelegateByteBuffer(buf);
        this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettyDatagramChannel.MESSAGE_INDEX);
        this.datagramPacket = new DatagramPacket(buf, channel.getSocketAddress());
        this.datagramPacket.retain();
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
            datagramPacket.release();
            datagramPacket = null;
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
        buffer.putInt(NettyDatagramChannel.SIZE_INDEX, size);
        buf.setIndex(0, NettyDatagramChannel.MESSAGE_INDEX + size);
        buf.retain(); //keep retain count up
        datagramPacket.retain();
        writer.run();
    }

}
