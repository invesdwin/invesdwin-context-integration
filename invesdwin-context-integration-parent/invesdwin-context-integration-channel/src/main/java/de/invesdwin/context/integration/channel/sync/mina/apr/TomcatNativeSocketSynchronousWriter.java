package de.invesdwin.context.integration.channel.sync.mina.apr;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
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
public class TomcatNativeSocketSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private TomcatNativeSocketSynchronousChannel channel;
    private long fd;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;

    public TomcatNativeSocketSynchronousWriter(final TomcatNativeSocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        fd = channel.getFileDescriptor();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MinaSocketSynchronousChannel.MESSAGE_INDEX);
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
            fd = -1;
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
            buffer.putInt(MinaSocketSynchronousChannel.SIZE_INDEX, size);
            writeFully(fd, buffer.nioByteBuffer(), 0, MinaSocketSynchronousChannel.MESSAGE_INDEX + size);
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    public static void writeFully(final long dst, final java.nio.ByteBuffer byteBuffer, final int pos, final int length)
            throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int position = pos;
        int remaining = length - pos;
        while (remaining > 0) {
            int count = Socket.sendb(dst, byteBuffer, position, remaining);
            if (count < 0) { // EOF
                if (Status.APR_STATUS_IS_EAGAIN(-count)) {
                    count = 0;
                } else if (Status.APR_STATUS_IS_EOF(-count)) {
                    count = 0;
                } else {
                    throw TomcatNativeSocketSynchronousChannel.newTomcatException(count);
                }
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
                position += count;
                remaining -= count;
            }
        }
        if (remaining > 0) {
            throw ByteBuffers.newEOF();
        }
    }

}