package de.invesdwin.context.integration.channel.sync.mina.unsafe.udp;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.transport.socket.apr.AprLibraryAccessor;
import org.apache.mina.transport.socket.apr.AprSession;
import org.apache.mina.transport.socket.apr.AprSessionAccessor;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.context.integration.channel.sync.mina.type.MinaSocketType;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class MinaNativeDatagramSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private MinaSocketSynchronousChannel channel;
    private IByteBuffer buffer;
    private long fd;
    private int position = 0;
    private long from;
    private long pool;

    public MinaNativeDatagramSynchronousReader(final MinaSocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        if (channel.getType() != MinaSocketType.AprUdp) {
            throw UnknownArgumentException.newInstance(IMinaSocketType.class, channel.getType());
        }
    }

    @Override
    public void open() throws IOException {
        channel.open(ch -> {
            //make sure Mina does not process any bytes
            ch.suspendWrite();
            ch.suspendRead();
            //validate connection
        }, true);
        final AprSession session = (AprSession) channel.getIoSession();
        fd = AprSessionAccessor.getDescriptor(session);
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateExpandable(channel.getSocketSize());

        pool = Pool.create(AprLibraryAccessor.getRootPool());
        try {
            from = Address.info("123.123.123.123", Socket.APR_INET, 0, 0, pool);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer = null;
            fd = 0;
            from = 0;
            Pool.destroy(pool);
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (position > 0) {
            return true;
        }
        final int count = Socket.recvfrom(from, fd, 0, buffer.asByteArray(), 0, channel.getSocketSize());
        if (count > 0) {
            position = count;
            return true;
        } else if (count < 0) {
            if (Status.APR_STATUS_IS_EAGAIN(-count)) {
                return false;
            }
            throw MinaSocketSynchronousChannel.newTomcatException(count);
        } else {
            return false;
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int targetPosition = MinaSocketSynchronousChannel.MESSAGE_INDEX;
        //read size
        try {
            while (position < targetPosition) {
                int count = Socket.recvfrom(from, fd, 0, buffer.asByteArray(), 0, channel.getSocketSize());
                if (count < 0) { // EOF
                    if (Status.APR_STATUS_IS_EOF(-count)) {
                        count = 0;
                    } else if (Status.APR_STATUS_IS_EAGAIN(-count)) {
                        count = 0;
                    } else {
                        close();
                        throw MinaSocketSynchronousChannel.newTomcatException(count);
                    }
                }
                if (count == 0 && timeout != null) {
                    if (zeroCountNanos == -1) {
                        zeroCountNanos = System.nanoTime();
                    } else if (timeout.isLessThanNanos(System.nanoTime() - zeroCountNanos)) {
                        close();
                        throw FastEOFException.getInstance("read timeout exceeded");
                    }
                    ASpinWait.onSpinWaitStatic();
                } else {
                    zeroCountNanos = -1L;
                    position += count;
                }
            }
        } catch (final ClosedChannelException e) {
            throw FastEOFException.getInstance(e);
        }
        final int size = buffer.getInt(MinaSocketSynchronousChannel.SIZE_INDEX);
        if (size <= 0) {
            close();
            throw FastEOFException.getInstance("non positive size");
        }
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - position;
        if (remaining > 0) {
            buffer.ensureCapacity(targetPosition);
            readFully(from, fd, buffer.asByteArray(), position, remaining);
        }
        position = 0;

        if (ClosedByteBuffer.isClosed(buffer, MinaSocketSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return buffer.slice(MinaSocketSynchronousChannel.MESSAGE_INDEX, size);
    }

    @Override
    public void readFinished() {
        //noop
    }

    public static void readFully(final long from, final long sock, final byte[] byteBuffer, final int pos,
            final int length) throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int position = pos;
        int remaining = length - pos;
        while (remaining > 0) {
            int count = Socket.recvfrom(from, sock, 0, byteBuffer, position, remaining);
            if (count < 0) { // EOF
                if (Status.APR_STATUS_IS_EOF(-count)) {
                    count = 0;
                } else if (Status.APR_STATUS_IS_EAGAIN(-count)) {
                    count = 0;
                } else {
                    throw MinaSocketSynchronousChannel.newTomcatException(count);
                }
            }
            if (count == 0 && timeout != null) {
                if (zeroCountNanos == -1) {
                    zeroCountNanos = System.nanoTime();
                } else if (timeout.isLessThanNanos(System.nanoTime() - zeroCountNanos)) {
                    throw FastEOFException.getInstance("read timeout exceeded");
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
