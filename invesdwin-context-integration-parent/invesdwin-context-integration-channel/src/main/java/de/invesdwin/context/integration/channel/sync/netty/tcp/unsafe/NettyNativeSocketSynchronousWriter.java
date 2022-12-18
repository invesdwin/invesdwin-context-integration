package de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.InputStreams;
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

    public NettyNativeSocketSynchronousWriter(final NettySocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        if (channel.isReaderRegistered()) {
            throw newNativeBidiNotSupportedException();
            //            channel.open(ch -> {
            //                ch.deregister();
            //            });
        } else {
            channel.open(ch -> {
                //make sure netty does not process any bytes
                ch.shutdownInput();
            });
            channel.getSocketChannel().deregister();
            final UnixChannel unixChannel = (UnixChannel) channel.getSocketChannel();
            channel.closeBootstrapAsync();
            fd = unixChannel.fd();
            //use direct buffer to prevent another copy from byte[] to native
            buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
            messageBuffer = new SlicedFromDelegateByteBuffer(buffer, NettySocketSynchronousChannel.MESSAGE_INDEX);
        }
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
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
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
    public void write(final IByteBufferProvider message) throws IOException {
        try {
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(NettySocketSynchronousChannel.SIZE_INDEX, size);
            writeFully(fd, buffer.nioByteBuffer(), 0, NettySocketSynchronousChannel.MESSAGE_INDEX + size);
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    public static void writeFully(final FileDescriptor dst, final java.nio.ByteBuffer byteBuffer, final int pos,
            final int length) throws IOException {
        try {
            int position = pos;
            int remaining = length - pos;
            int tries = 0;
            while (remaining > 0) {
                final int count = dst.write(byteBuffer, position, remaining);
                if (count == -1) { // EOF
                    break;
                }
                position += count;
                remaining -= count;
                tries++;
                if (tries > InputStreams.MAX_READ_FULLY_TRIES) {
                    throw FastEOFException.getInstance("write tries exceeded");
                }
            }
            if (remaining > 0) {
                throw ByteBuffers.newPutBytesToEOF();
            }
        } catch (final ClosedChannelException e) {
            throw FastEOFException.getInstance(e);
        }
    }

}
