package de.invesdwin.context.integration.channel.sync.jucx.stream;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.openucx.jucx.ucp.UcpConstants;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRequest;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@NotThreadSafe
public class JucxStreamSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private JucxStreamSynchronousChannel channel;
    private UcpMemory memory;
    private IByteBuffer buffer;
    private int position = 0;
    private int bufferOffset = 0;
    private int messageTargetPosition = 0;
    private UcpRequest request;

    public JucxStreamSynchronousReader(final JucxStreamSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        memory = channel.getUcpContext().memoryMap(channel.getUcpMemMapParams());
        buffer = new UnsafeByteBuffer(memory.getAddress(), channel.getSocketSize());
    }

    @Override
    public void close() {
        if (buffer != null) {
            memory.close();
            buffer = null;
            position = 0;
            bufferOffset = 0;
            messageTargetPosition = 0;
            request = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (buffer == null) {
            throw FastEOFException.getInstance("socket closed");
        }
        return hasMessage();
    }

    private boolean hasMessage() throws IOException {
        if (messageTargetPosition == 0) {
            final int sizeTargetPosition = bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX;
            //allow reading further than required to reduce the syscalls if possible
            if (!readFurther(sizeTargetPosition, buffer.remaining(position))) {
                return false;
            }
            final int size = buffer.getInt(bufferOffset + SocketSynchronousChannel.SIZE_INDEX);
            if (size <= 0) {
                close();
                throw FastEOFException.getInstance("non positive size");
            }
            this.messageTargetPosition = sizeTargetPosition + size;
            buffer.ensureCapacity(messageTargetPosition);
        }
        /*
         * only read as much further as required, so that we have a message where we can reset the position to 0 so the
         * expandable buffer does not grow endlessly due to fragmented messages piling up at the end each time.
         */
        return readFurther(messageTargetPosition, messageTargetPosition - position);
    }

    private boolean readFurther(final int targetPosition, final int readLength) throws IOException {
        if (position < targetPosition) {
            if (request == null) {
                request = channel.getUcpEndpoint()
                        .recvStreamNonBlocking(buffer.addressOffset() + position, readLength,
                                UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, channel.getErrorUcxCallback().reset());
            }
            try {
                channel.getUcpWorker().progressRequest(request);
            } catch (final Exception e) {
                throw new IOException(e);
            }
            channel.getErrorUcxCallback().maybeThrow();
            if (request.isCompleted()) {
                position += request.getRecvSize();
                request = null;
            }
        }
        return position >= targetPosition;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int size = messageTargetPosition - bufferOffset - SocketSynchronousChannel.MESSAGE_INDEX;
        if (ClosedByteBuffer.isClosed(buffer, bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }

        final IByteBuffer message = buffer.slice(bufferOffset + SocketSynchronousChannel.MESSAGE_INDEX, size);
        final int offset = SocketSynchronousChannel.MESSAGE_INDEX + size;
        if (position > (bufferOffset + offset)) {
            /*
             * can be a maximum of a few messages we read like this because of the size in hasNext, the next read in
             * hasNext will be done with position 0
             */
            bufferOffset += offset;
        } else {
            bufferOffset = 0;
            position = 0;
        }
        messageTargetPosition = 0;
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

}
