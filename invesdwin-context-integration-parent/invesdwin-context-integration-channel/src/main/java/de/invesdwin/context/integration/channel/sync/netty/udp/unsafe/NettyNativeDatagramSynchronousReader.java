package de.invesdwin.context.integration.channel.sync.netty.udp.unsafe;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe.NettyNativeSocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe.NettyNativeSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.UnixChannel;

@NotThreadSafe
public class NettyNativeDatagramSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    public static final boolean SERVER = true;
    private final int socketSize;
    private NettyDatagramSynchronousChannel channel;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private FileDescriptor fd;
    private int position = 0;

    public NettyNativeDatagramSynchronousReader(final INettyDatagramChannelType type,
            final InetSocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this(new NettyDatagramSynchronousChannel(type, socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public NettyNativeDatagramSynchronousReader(final NettyDatagramSynchronousChannel channel) {
        this.channel = channel;
        if (channel.isServer() != SERVER) {
            throw new IllegalStateException("datagram reader has to be the server");
        }
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        if (channel.isWriterRegistered()) {
            throw NettyNativeSocketSynchronousWriter.newNativeBidiNotSupportedException();
        } else {
            channel.open(bootstrap -> {
                bootstrap.handler(new ChannelInboundHandlerAdapter());
            }, null);
            channel.getDatagramChannel().deregister();
            final UnixChannel unixChannel = (UnixChannel) channel.getDatagramChannel();
            channel.closeBootstrapAsync();
            fd = unixChannel.fd();
            //use direct buffer to prevent another copy from byte[] to native
            buffer = ByteBuffers.allocateDirectExpandable(socketSize);
            messageBuffer = buffer.asNioByteBuffer(0, socketSize);
        }
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer = null;
            messageBuffer = null;
            fd = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (position > 0) {
            return true;
        }
        try {
            final int count = fd.read(messageBuffer, 0, socketSize);
            if (count > 0) {
                position = count;
                return true;
            } else if (count < 0) {
                throw FastEOFException.getInstance("closed by other side");
            } else {
                return false;
            }
        } catch (final ClosedChannelException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int targetPosition = NettySocketSynchronousChannel.MESSAGE_INDEX;
        //read size
        try {
            while (position < targetPosition) {
                final int count = fd.read(messageBuffer, 0, socketSize);
                if (count < 0) { // EOF
                    close();
                    throw ByteBuffers.newEOF();
                }
                if (count == 0 && timeout != null) {
                    if (zeroCountNanos == -1) {
                        zeroCountNanos = System.nanoTime();
                    } else if (timeout.isLessThanNanos(System.nanoTime() - zeroCountNanos)) {
                        close();
                        throw FastEOFException.getInstance("read timeout exceeded");
                    }
                } else {
                    zeroCountNanos = -1L;
                    position += count;
                }
            }
        } catch (final ClosedChannelException e) {
            throw FastEOFException.getInstance(e);
        }
        final int size = buffer.getInt(NettySocketSynchronousChannel.SIZE_INDEX);
        if (size <= 0) {
            close();
            throw FastEOFException.getInstance("non positive size");
        }
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - position;
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            buffer.ensureCapacity(targetPosition);
            if (buffer.capacity() != capacityBefore) {
                messageBuffer = buffer.asNioByteBuffer(0, socketSize);
            }
            NettyNativeSocketSynchronousReader.readFully(fd, messageBuffer, position, remaining);
        }
        position = 0;

        if (ClosedByteBuffer.isClosed(buffer, NettySocketSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return buffer.slice(NettySocketSynchronousChannel.MESSAGE_INDEX, size);
    }

    @Override
    public void readFinished() {
        //noop
    }

}
