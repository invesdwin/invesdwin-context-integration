package de.invesdwin.context.integration.channel.netty.udp;

import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.Unpooled;

@NotThreadSafe
public class NettyDatagramSynchronousWriter extends ANettyDatagramSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public NettyDatagramSynchronousWriter(final INettyDatagramChannelType type, final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        //netty uses direct buffer per default
        buffer = new NettyDelegateByteBuffer(Unpooled.buffer(socketSize));
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
    }

    @Override
    public void close() {
        if (buffer != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
            messageBuffer = null;
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) {
        final int size = message.write(messageBuffer);
        buffer.putInt(SIZE_INDEX, size);
        datagramChannel.writeAndFlush(buffer.getDelegate());
    }

}
