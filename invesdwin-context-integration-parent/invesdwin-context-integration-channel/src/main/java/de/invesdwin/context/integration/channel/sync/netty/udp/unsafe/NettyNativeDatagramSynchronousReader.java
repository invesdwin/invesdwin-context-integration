package de.invesdwin.context.integration.channel.sync.netty.udp.unsafe;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe.NettyNativeSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.unix.Socket;
import io.netty.channel.unix.UnixChannel;

@NotThreadSafe
public class NettyNativeDatagramSynchronousReader implements ISynchronousReader<IByteBuffer> {

    public static final boolean SERVER = true;
    private final NettyDatagramChannel channel;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private Socket fd;
    private int position = 0;

    public NettyNativeDatagramSynchronousReader(final INettyDatagramChannelType type,
            final InetSocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this(new NettyDatagramChannel(type, socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public NettyNativeDatagramSynchronousReader(final NettyDatagramChannel channel) {
        this.channel = channel;
        if (channel.isServer() != SERVER) {
            throw new IllegalStateException("datagram reader has to be the server");
        }
        this.channel.setReaderRegistered();
    }

    @Override
    public void open() throws IOException {
        if (channel.isWriterRegistered()) {
            throw NettyNativeSocketSynchronousWriter.newNativeBidiNotSupportedException();
        }
        channel.open(bootstrap -> {
            bootstrap.handler(new ChannelInboundHandlerAdapter());
        }, ch -> {
            ch.deregister();
        });
        final UnixChannel unixChannel = (UnixChannel) channel.getDatagramChannel();
        channel.closeBootstrapAsync();
        fd = (Socket) unixChannel.fd();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = buffer.asNioByteBuffer(0, channel.getSocketSize());
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer = null;
            messageBuffer = null;
            fd = null;
        }
        channel.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (position > 0) {
            return true;
        }
        final int read = fd.read(messageBuffer, 0, channel.getSocketSize());
        if (read > 0) {
            position = read;
            return true;
        } else if (read < 0) {
            throw new EOFException("closed by other side");
        } else {
            return false;
        }
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        int targetPosition = NettySocketChannel.MESSAGE_INDEX;
        int size = 0;
        //read size
        while (position < targetPosition) {
            final int read = fd.read(messageBuffer, 0, channel.getSocketSize());
            position += read;
        }
        size = buffer.getInt(NettySocketChannel.SIZE_INDEX);
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - position;
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            buffer.ensureCapacity(targetPosition);
            if (buffer.capacity() != capacityBefore) {
                messageBuffer = buffer.asNioByteBuffer(0, channel.getSocketSize());
            }
            readFully(fd, messageBuffer, position, remaining);
        }
        position = 0;

        if (ClosedByteBuffer.isClosed(buffer, NettySocketChannel.MESSAGE_INDEX, size)) {
            close();
            throw new EOFException("closed by other side");
        }
        return buffer.slice(NettySocketChannel.MESSAGE_INDEX, size);
    }

    @Override
    public void readFinished() {
        //noop
    }

    public static void readFully(final Socket src, final java.nio.ByteBuffer byteBuffer, final int pos,
            final int length) throws IOException {
        int position = pos;
        int remaining = length - pos;
        while (remaining > 0) {
            final int count = src.read(byteBuffer, position, remaining);
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
