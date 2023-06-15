package de.invesdwin.context.integration.channel.sync.netty.udp.unsafe;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe.NettyNativeSocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe.NettyNativeSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
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
    private int bufferOffset = 0;
    private int messageTargetPosition = 0;

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
            messageBuffer = buffer.asNioByteBuffer();
        }
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer = null;
            messageBuffer = null;
            fd = null;
            position = 0;
            bufferOffset = 0;
            messageTargetPosition = 0;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (buffer == null) {
            throw FastEOFException.getInstance("socket closed");
        }
        return hasMessage();
    }

    private boolean hasMessage() throws IOException {
        if (messageTargetPosition == 0) {
            final int sizeTargetPosition = bufferOffset + NettyDatagramSynchronousChannel.MESSAGE_INDEX;
            //allow reading further than required to reduce the syscalls if possible
            if (!readFurther(sizeTargetPosition, buffer.remaining(position))) {
                return false;
            }
            final int size = buffer.getInt(bufferOffset + NettyDatagramSynchronousChannel.SIZE_INDEX);
            if (size <= 0) {
                close();
                throw FastEOFException.getInstance("non positive size: %s", size);
            }
            this.messageTargetPosition = sizeTargetPosition + size;
            if (buffer.capacity() < messageTargetPosition) {
                buffer.ensureCapacity(messageTargetPosition);
                messageBuffer = buffer.asNioByteBuffer();
            }
        }
        /*
         * only read as much further as required, so that we have a message where we can reset the position to 0 so the
         * expandable buffer does not grow endlessly due to fragmented messages piling up at the end each time.
         */
        return readFurther(messageTargetPosition, messageTargetPosition - position);
    }

    private boolean readFurther(final int targetPosition, final int readLength) throws IOException {
        if (position < targetPosition) {
            position += NettyNativeSocketSynchronousReader.read0(fd, messageBuffer, position, readLength);
        }
        return position >= targetPosition;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int size = messageTargetPosition - bufferOffset - NettyDatagramSynchronousChannel.MESSAGE_INDEX;
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + NettyDatagramSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }

        final IByteBuffer message = buffer.slice(bufferOffset + NettyDatagramSynchronousChannel.MESSAGE_INDEX, size);
        final int offset = NettyDatagramSynchronousChannel.MESSAGE_INDEX + size;
        if (position > (bufferOffset + offset)) {
            /*
             * can be a maximum of a few messages we read like this because of the size in hasNext, the next read in
             * hasNext will be done with position 0
             */
            bufferOffset += offset;
        } else {
            bufferOffset = 0;
            position = 0;
        }
        messageTargetPosition = 0;
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

}
