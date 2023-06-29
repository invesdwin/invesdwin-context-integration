package de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.UnixChannel;

@NotThreadSafe
public class NettyNativeSocketSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private NettySocketSynchronousChannel channel;
    private FileDescriptor fd;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private java.nio.ByteBuffer messageToWrite;
    private int position;
    private int remaining;

    public NettyNativeSocketSynchronousWriter(final NettySocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open(ch -> {
            //make sure netty does not process any bytes
            if (!channel.isReaderRegistered()) {
                ch.shutdownInput();
            }
        });
        channel.getSocketChannel().deregister();
        final UnixChannel unixChannel = (UnixChannel) channel.getSocketChannel();
        channel.closeBootstrapAsync();
        fd = unixChannel.fd();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettySocketSynchronousChannel.MESSAGE_INDEX);
    }

    public static UnsupportedOperationException newNativeBidiNotSupportedException() {
        //io.netty.channel.unix.Errors$NativeIoException: write(..) failed: Datenübergabe unterbrochen (broken pipe)
        return new UnsupportedOperationException(
                "Native bidirectional mode for reader/writer on same channel not supported. Please use separate channels for native reader/writer. FileDescriptor reads/writer will cause broken pipes otherwise.");
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            try {
                writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
            messageBuffer = null;
            fd = null;
            messageToWrite = null;
            position = 0;
            remaining = 0;
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
            return true;
        } else {
            return false;
        }
    }

    private boolean writeFurther() throws IOException {
        final int count = fd.write(messageToWrite, position, remaining);
        remaining -= count;
        position += count;
        return remaining > 0;
    }

}
