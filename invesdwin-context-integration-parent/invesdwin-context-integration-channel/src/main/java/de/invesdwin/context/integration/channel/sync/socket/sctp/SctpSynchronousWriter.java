package de.invesdwin.context.integration.channel.sync.socket.sctp;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class SctpSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private SctpSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private SctpChannel socketChannel;
    private MessageInfo outMessageInfo;

    public SctpSynchronousWriter(final SctpSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, SctpSynchronousChannel.MESSAGE_INDEX);
        socketChannel = channel.getSocketChannel();

        outMessageInfo = MessageInfo.createOutgoing(null, 0);
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
        outMessageInfo = null;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        try {
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(SctpSynchronousChannel.SIZE_INDEX, size);
            writeFully(socketChannel, buffer.asNioByteBuffer(0, SctpSynchronousChannel.MESSAGE_INDEX + size),
                    outMessageInfo, 0, size);
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    public static void writeFully(final SctpChannel dst, final java.nio.ByteBuffer byteBuffer,
            final MessageInfo outMessageInfo, final int pos, final int length) throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int remaining = length - pos;
        try {
            while (remaining > 0) {
                final int count = dst.send(byteBuffer, outMessageInfo);
                if (count < 0) { // EOF
                    break;
                }
                if (count == 0 && timeout != null) {
                    if (zeroCountNanos == -1) {
                        zeroCountNanos = System.nanoTime();
                    } else if (timeout.isLessThanNanos(System.nanoTime() - zeroCountNanos)) {
                        throw FastEOFException.getInstance("write timeout exceeded");
                    }
                    ASpinWait.onSpinWaitStatic();
                } else {
                    zeroCountNanos = -1L;
                    remaining -= count;
                }
            }
            if (remaining > 0) {
                throw ByteBuffers.newEOF();
            }
        } finally {
            byteBuffer.clear();
        }
    }

}