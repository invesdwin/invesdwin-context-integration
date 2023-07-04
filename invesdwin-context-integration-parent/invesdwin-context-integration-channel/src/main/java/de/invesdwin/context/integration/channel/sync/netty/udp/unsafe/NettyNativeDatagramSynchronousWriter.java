package de.invesdwin.context.integration.channel.sync.netty.udp.unsafe;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udp.NettyDatagramSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.unix.Socket;
import io.netty.channel.unix.UnixChannel;

@NotThreadSafe
public class NettyNativeDatagramSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final int socketSize;
    private NettyDatagramSynchronousChannel channel;
    private Socket fd;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private java.nio.ByteBuffer messageToWrite;
    private int position;
    private int remaining;
    private InetSocketAddress recipient;

    public NettyNativeDatagramSynchronousWriter(final NettyDatagramSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        //        if (channel.isReaderRegistered()) {
        //            throw NettyNativeSocketSynchronousWriter.newNativeBidiNotSupportedException();
        //        } else {
        channel.open(bootstrap -> {
            bootstrap.handler(new ChannelInboundHandlerAdapter());
        }, null);
        channel.getDatagramChannel().deregister();
        final UnixChannel unixChannel = (UnixChannel) channel.getDatagramChannel();
        channel.closeBootstrapAsync();
        fd = (Socket) unixChannel.fd();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettySocketSynchronousChannel.MESSAGE_INDEX);
        //        }
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            if (!channel.isServer() || !channel.isMultipleClientsAllowed() && recipient != null) {
                try {
                    writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
                } catch (final Throwable t) {
                    //ignore
                }
            }
            buffer = null;
            messageBuffer = null;
            fd = null;
            messageToWrite = null;
            position = 0;
            remaining = 0;
            recipient = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        try {
            if (channel.isServer()) {
                recipient = channel.getOtherSocketAddress();
            } else {
                recipient = channel.getSocketAddress();
            }
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(NettySocketSynchronousChannel.SIZE_INDEX, size);
            messageToWrite = buffer.asNioByteBuffer();
            position = 0;
            remaining = NettySocketSynchronousChannel.MESSAGE_INDEX + size;
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (messageToWrite == null) {
            return true;
        } else if (!writeFurther()) {
            messageToWrite = null;
            position = 0;
            remaining = 0;
            recipient = null;
            return true;
        } else {
            return false;
        }
    }

    private boolean writeFurther() throws IOException {
        final int count = fd.sendTo(messageToWrite, position, remaining, recipient.getAddress(), recipient.getPort(),
                false);
        remaining -= count;
        position += count;
        return remaining > 0;
    }

}
