package de.invesdwin.context.integration.channel.sync.socket.sctp;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import javax.annotation.concurrent.NotThreadSafe;

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
    private Object socketChannel;
    private Object outMessageInfo;

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

        try {
            outMessageInfo = SctpSynchronousChannel.MESSAGEINFO_CREATEOUTGOING_METHOD.invoke(null, null, 0);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
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
            socketChannel = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
        outMessageInfo = null;
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
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

    @Override
    public boolean writeFlushed() {
        return true;
    }

    /**
     * Old, blocking variation of the write
     */
    public static void writeFully(final Object dst, final java.nio.ByteBuffer byteBuffer, final Object outMessageInfo,
            final int pos, final int length) throws IOException {
        //System.out.println("TODO non-blocking");
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int remaining = length - pos;
        try {
            while (remaining > 0) {
                final int count = (int) SctpSynchronousChannel.SCTPCHANNEL_SEND_METHOD.invoke(dst, byteBuffer,
                        outMessageInfo);
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
        } catch (final IOException e) {
            throw e;
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        } finally {
            byteBuffer.clear();
        }
    }

}
