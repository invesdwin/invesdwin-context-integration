package de.invesdwin.context.integration.channel.sync.chronicle.network;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.time.duration.Duration;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

@NotThreadSafe
public class ChronicleNetworkSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private ChronicleNetworkSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private ChronicleSocketChannel socketChannel;

    public ChronicleNetworkSynchronousWriter(final ChronicleNetworkSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        if (!channel.isReaderRegistered()) {
            channel.getSocket().shutdownInput();
        }
        socketChannel = channel.getSocketChannel();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, ChronicleNetworkSynchronousChannel.MESSAGE_INDEX);
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
            socketChannel = null;
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
            buffer.putInt(ChronicleNetworkSynchronousChannel.SIZE_INDEX, size);
            writeFully(socketChannel,
                    buffer.asNioByteBuffer(0, ChronicleNetworkSynchronousChannel.MESSAGE_INDEX + size));
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    public static void writeFully(final ChronicleSocketChannel dst, final java.nio.ByteBuffer byteBuffer)
            throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int remaining = byteBuffer.remaining();
        final int positionBefore = byteBuffer.position();
        while (remaining > 0) {
            final int count = dst.write(byteBuffer);
            if (count < 0) { // EOF
                break;
            }
            if (count == 0 && timeout != null) {
                if (zeroCountNanos == -1) {
                    zeroCountNanos = System.nanoTime();
                } else if (timeout.isLessThanNanos(System.nanoTime() - zeroCountNanos)) {
                    throw FastEOFException.getInstance("write timeout exceeded");
                }
            } else {
                zeroCountNanos = -1L;
                remaining -= count;
            }
        }
        ByteBuffers.position(byteBuffer, positionBefore);
        if (remaining > 0) {
            throw ByteBuffers.newEOF();
        }
    }

}
