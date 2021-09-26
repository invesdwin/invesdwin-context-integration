package de.invesdwin.context.integration.channel.sync.netty.tcp.nativee;

import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.UnixChannel;

@NotThreadSafe
public class NettyNativeSocketSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

    private final NettySocketChannel channel;
    private FileDescriptor fd;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public NettyNativeSocketSynchronousWriter(final INettySocketChannelType type, final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        this(new NettySocketChannel(type, socketAddress, server, estimatedMaxMessageSize));
    }

    public NettyNativeSocketSynchronousWriter(final NettySocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open(ch -> {
            //make sure netty does not process any bytes
            ch.shutdownInput();
            ch.shutdownInput();
        });
        channel.getSocketChannel().deregister();
        final UnixChannel unixChannel = (UnixChannel) channel.getSocketChannel();
        channel.closeBootstrapAsync();
        fd = unixChannel.fd();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettySocketChannel.MESSAGE_INDEX);
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
            messageBuffer = null;
            fd = null;
        }
        channel.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        try {
            final int size = message.write(messageBuffer);
            buffer.putInt(NettySocketChannel.SIZE_INDEX, size);
            writeFully(fd, buffer.nioByteBuffer(), 0, NettySocketChannel.MESSAGE_INDEX + size);
        } catch (final IOException e) {
            throw NettySocketChannel.newEofException(e);
        }
    }

    public static void writeFully(final FileDescriptor dst, final java.nio.ByteBuffer byteBuffer, final int pos,
            final int length) throws IOException {
        int position = pos;
        int remaining = length - pos;
        while (remaining > 0) {
            final int count = dst.write(byteBuffer, position, remaining);
            if (count == -1) { // EOF
                break;
            }
            position += count;
            remaining -= count;
        }
        if (remaining > 0) {
            throw ByteBuffers.newPutBytesToEOF();
        }
    }

}
