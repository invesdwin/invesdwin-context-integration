package de.invesdwin.context.integration.channel.sync.netty.udp.unsafe;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe.NettyNativeSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.unix.Socket;
import io.netty.channel.unix.UnixChannel;

@NotThreadSafe
public class NettyNativeDatagramSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

    public static final boolean SERVER = false;
    private final NettyDatagramChannel channel;
    private Socket fd;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public NettyNativeDatagramSynchronousWriter(final INettyDatagramChannelType type,
            final InetSocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this(new NettyDatagramChannel(type, socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public NettyNativeDatagramSynchronousWriter(final NettyDatagramChannel channel) {
        this.channel = channel;
        if (channel.isServer() != SERVER) {
            throw new IllegalStateException("datagram writer has to be the client");
        }
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        if (channel.isReaderRegistered()) {
            throw NettyNativeSocketSynchronousWriter.newNativeDuplexNotSupportedException();
        }
        channel.open(bootstrap -> {
            bootstrap.handler(new ChannelInboundHandlerAdapter());
        }, null);
        channel.getDatagramChannel().deregister();
        final UnixChannel unixChannel = (UnixChannel) channel.getDatagramChannel();
        channel.closeBootstrapAsync();
        fd = (Socket) unixChannel.fd();
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
            final int size = message.writeBuffer(messageBuffer);
            buffer.putInt(NettySocketChannel.SIZE_INDEX, size);
            writeFully(fd, buffer.nioByteBuffer(), 0, NettySocketChannel.MESSAGE_INDEX + size,
                    channel.getSocketAddress(), false);
        } catch (final IOException e) {
            throw NettySocketChannel.newEofException(e);
        }
    }

    public static void writeFully(final Socket dst, final java.nio.ByteBuffer byteBuffer, final int pos,
            final int length, final InetSocketAddress recipient, final boolean fastOpen) throws IOException {
        int position = pos;
        int remaining = length - pos;
        while (remaining > 0) {
            final int count = dst.sendTo(byteBuffer, position, remaining, recipient.getAddress(), recipient.getPort(),
                    fastOpen);
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
