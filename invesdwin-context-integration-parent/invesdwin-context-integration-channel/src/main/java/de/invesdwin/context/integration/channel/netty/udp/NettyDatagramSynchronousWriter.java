package de.invesdwin.context.integration.channel.netty.udp;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.DatagramPacket;

@NotThreadSafe
public class NettyDatagramSynchronousWriter extends ANettyDatagramSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private ByteBuf buf;
    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private DatagramPacket datagramPacket;

    public NettyDatagramSynchronousWriter(final INettyDatagramChannelType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        //netty uses direct buffer per default
        this.buf = Unpooled.directBuffer(socketSize);
        buf.retain();
        this.buffer = new NettyDelegateByteBuffer(buf);
        this.messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
        this.datagramPacket = new DatagramPacket(buf, socketAddress);
        this.datagramPacket.retain();
    }

    @Override
    public void close() {
        if (buffer != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
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
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) {
        final int size = message.write(messageBuffer);
        buffer.putInt(SIZE_INDEX, size);
        buf.setIndex(0, MESSAGE_INDEX + size);
        datagramChannel.writeAndFlush(datagramPacket);
    }

}
