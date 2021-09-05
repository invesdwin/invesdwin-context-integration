package de.invesdwin.context.integration.channel.netty;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.netty.type.INettyChannelType;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

@NotThreadSafe
public class NettySynchronousReader<M> extends ANettySynchronousChannel implements ISynchronousReader<M> {

    private final ISerde<M> messageSerde;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;

    public NettySynchronousReader(final INettyChannelType type, final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize, final ISerde<M> messageSerde) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
        this.messageSerde = messageSerde;
    }

    @Override
    public void open() throws IOException {
        super.open();
        socketChannel.shutdownOutput();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(estimatedMaxMessageSize);
        messageBuffer = buffer.asByteBuffer(0, socketSize);
        socketChannel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
                super.channelRead(ctx, msg);
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer = null;
            messageBuffer = null;
        }
        super.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (messageBuffer.position() > 0) {
            return true;
        }
        final int read = socketChannel.read(messageBuffer);
        return read > 0;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        int targetPosition = MESSAGE_INDEX;
        int size = 0;
        //read size
        while (messageBuffer.position() < targetPosition) {
            socketChannel.read(messageBuffer);
        }
        size = buffer.getInt(SIZE_INDEX);
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - messageBuffer.position();
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            buffer.putBytesTo(messageBuffer.position(), socketChannel, remaining);
            if (buffer.capacity() != capacityBefore) {
                messageBuffer = buffer.asByteBuffer(0, socketSize);
            }
        }

        ByteBuffers.position(messageBuffer, 0);
        if (ClosedByteBuffer.isClosed(buffer, MESSAGE_INDEX, size)) {
            close();
            throw new EOFException("closed by other side");
        }
        return buffer.slice(MESSAGE_INDEX, size);
    }

}
