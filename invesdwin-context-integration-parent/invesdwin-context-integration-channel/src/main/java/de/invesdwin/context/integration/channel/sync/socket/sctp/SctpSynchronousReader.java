package de.invesdwin.context.integration.channel.sync.socket.sctp;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class SctpSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private SctpSynchronousChannel channel;
    private final int socketSize;
    private IByteBuffer buffer;
    private java.nio.ByteBuffer messageBuffer;
    private int bufferOffset = 0;
    private SctpChannel socketChannel;

    public SctpSynchronousReader(final SctpSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //remove volatile access
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = buffer.asNioByteBuffer(0, socketSize);
        socketChannel = channel.getSocketChannel();
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        messageBuffer = null;
        socketChannel = null;
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (messageBuffer.position() > 0) {
            return true;
        }
        final MessageInfo messageInfo = socketChannel.receive(messageBuffer, null, null);
        if (messageInfo == null) {
            return false;
        }
        final int count = messageInfo.bytes();
        return count > 0;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        int targetPosition = bufferOffset + SctpSynchronousChannel.MESSAGE_INDEX;
        //read size
        while (messageBuffer.position() < targetPosition) {
            final MessageInfo messageInfo = socketChannel.receive(messageBuffer, null, null);
            final int count = messageInfo.bytes();
            if (count < 0) { // EOF
                close();
                throw ByteBuffers.newEOF();
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
            }
        }
        final int size = buffer.getInt(bufferOffset + SctpSynchronousChannel.SIZE_INDEX);
        if (size <= 0) {
            close();
            throw FastEOFException.getInstance("non positive size");
        }
        targetPosition += size;
        //read message if not complete yet
        final int remaining = targetPosition - messageBuffer.position();
        if (remaining > 0) {
            final int capacityBefore = buffer.capacity();
            buffer.ensureCapacity(targetPosition);
            if (buffer.capacity() != capacityBefore) {
                final int positionBefore = messageBuffer.position();
                messageBuffer = buffer.asNioByteBuffer(0, buffer.capacity());
                ByteBuffers.position(messageBuffer, positionBefore);
            }
            readFully(socketChannel, messageBuffer, messageBuffer.position(), remaining);
        }

        final int offset = SctpSynchronousChannel.MESSAGE_INDEX + size;
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + SctpSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        final IByteBuffer message = buffer.slice(bufferOffset + SctpSynchronousChannel.MESSAGE_INDEX, size);
        if (messageBuffer.position() > (bufferOffset + offset)) {
            /*
             * can be a maximum of a few messages we read like this because of the size in hasNext, the next read in
             * hasNext will be done with position 0
             */
            bufferOffset += offset;
        } else {
            bufferOffset = 0;
            ByteBuffers.position(messageBuffer, 0);
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

    public static void readFully(final SctpChannel src, final java.nio.ByteBuffer byteBuffer, final int pos,
            final int length) throws IOException {
        final Duration timeout = URIs.getDefaultNetworkTimeout();
        long zeroCountNanos = -1L;

        ByteBuffers.position(byteBuffer, pos);
        byteBuffer.limit(pos + length);
        int remaining = length - pos;
        try {
            while (remaining > 0) {
                final MessageInfo messageInfo = src.receive(byteBuffer, null, null);
                final int count = messageInfo.bytes();
                if (count < 0) { // EOF
                    break;
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