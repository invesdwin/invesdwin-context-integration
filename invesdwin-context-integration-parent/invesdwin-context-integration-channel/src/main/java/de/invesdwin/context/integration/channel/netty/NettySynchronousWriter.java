package de.invesdwin.context.integration.channel.netty;

import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.netty.type.INettyChannelType;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.NettyDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.buffer.Unpooled;

@NotThreadSafe
public class NettySynchronousWriter extends ANettySynchronousChannel implements ISynchronousWriter<IByteBufferWriter> {

    private NettyDelegateByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public NettySynchronousWriter(final INettyChannelType type, final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        socketChannel.shutdownInput();
        //use direct buffer to prevent another copy from byte[] to native
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
        socketChannel.writeAndFlush(buffer.getDelegate());
    }

}
