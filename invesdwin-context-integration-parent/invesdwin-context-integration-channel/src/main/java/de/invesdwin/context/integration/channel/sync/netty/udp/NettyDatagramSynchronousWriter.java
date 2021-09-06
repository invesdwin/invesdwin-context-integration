package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.DatagramPacket;

@NotThreadSafe
public class NettyDatagramSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

    private NettyDatagramChannel channel;
    private ByteBuf buf;
    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private DatagramPacket datagramPacket;

    public NettyDatagramSynchronousWriter(final NettyDatagramChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open(null);
        //netty uses direct buffer per default
        this.buf = Unpooled.directBuffer(channel.getSocketSize());
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
                writeFuture(ClosedByteBuffer.INSTANCE).sync();
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

    private ChannelFuture writeFuture(final IByteBufferWriter message) {
        final int size = message.write(messageBuffer);
        buffer.putInt(NettyDatagramChannel.SIZE_INDEX, size);
        buf.setIndex(0, NettyDatagramChannel.MESSAGE_INDEX + size);
        buf.retain(); //keep retain count up
        return channel.getDatagramChannel().writeAndFlush(datagramPacket);
    }

}
