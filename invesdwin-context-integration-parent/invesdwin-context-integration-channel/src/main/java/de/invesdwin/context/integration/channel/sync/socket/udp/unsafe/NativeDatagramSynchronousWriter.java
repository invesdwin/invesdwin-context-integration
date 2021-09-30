package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.udp.ADatagramSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import net.openhft.chronicle.core.Jvm;

@NotThreadSafe
public class NativeDatagramSynchronousWriter extends ADatagramSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    public static final boolean SERVER = false;
    private IByteBuffer buffer;
    private IByteBuffer messageBuffer;
    private FileDescriptor fd;

    public NativeDatagramSynchronousWriter(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        super(socketAddress, SERVER, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
        fd = Jvm.getValue(socket.getChannel(), "fd");
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
            messageBuffer = null;
            fd = null;
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        final int size = message.write(messageBuffer);
        buffer.putInt(SIZE_INDEX, size);
        NativeSocketSynchronousWriter.writeFully(fd, buffer.addressOffset(), 0, MESSAGE_INDEX + size);
    }

}
