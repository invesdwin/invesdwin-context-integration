package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import net.openhft.chronicle.core.Jvm;

@NotThreadSafe
public class NativeDatagramSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

    public static final boolean SERVER = false;
    private DatagramSynchronousChannel channel;
    private IByteBuffer buffer;
    private IByteBuffer messageBuffer;
    private FileDescriptor fd;

    public NativeDatagramSynchronousWriter(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        this(new DatagramSynchronousChannel(socketAddress, SERVER, estimatedMaxMessageSize));
    }

    public NativeDatagramSynchronousWriter(final DatagramSynchronousChannel channel) {
        this.channel = channel;
        if (channel.isServer() != SERVER) {
            throw new IllegalStateException("datagram writer has to be the client");
        }
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, DatagramSynchronousChannel.MESSAGE_INDEX);
        fd = Jvm.getValue(channel.getSocketChannel(), "fd");
    }

    @Override
    public void close() throws IOException {
        if (fd != null) {
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
    public void write(final IByteBufferWriter message) throws IOException {
        final int size = message.writeBuffer(messageBuffer);
        buffer.putInt(DatagramSynchronousChannel.SIZE_INDEX, size);
        NativeSocketSynchronousWriter.writeFully(fd, buffer.addressOffset(), 0,
                DatagramSynchronousChannel.MESSAGE_INDEX + size);
    }

}
