package de.invesdwin.context.integration.channel.sync.chronicle.network;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.chronicle.network.type.ChronicleSocketChannelType;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

@NotThreadSafe
public class ChronicleNetworkSynchronousWriter extends AChronicleNetworkSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public ChronicleNetworkSynchronousWriter(final ChronicleSocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        socket.shutdownInput();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
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
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        try {
            final int size = message.writeBuffer(messageBuffer);
            buffer.putInt(SIZE_INDEX, size);
            writeFully(socketChannel, buffer.asNioByteBuffer(0, MESSAGE_INDEX + size));
        } catch (final IOException e) {
            throw newEofException(e);
        }
    }

    public static void writeFully(final ChronicleSocketChannel dst, final java.nio.ByteBuffer byteBuffer)
            throws IOException {
        int remaining = byteBuffer.remaining();
        while (remaining > 0) {
            final int count = dst.write(byteBuffer);
            if (count == -1) { // EOF
                break;
            }
            remaining -= count;
        }
        if (remaining > 0) {
            throw ByteBuffers.newPutBytesToEOF();
        }
    }

}
