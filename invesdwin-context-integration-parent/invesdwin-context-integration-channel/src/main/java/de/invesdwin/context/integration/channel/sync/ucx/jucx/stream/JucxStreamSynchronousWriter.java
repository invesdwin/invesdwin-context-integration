package de.invesdwin.context.integration.channel.sync.ucx.jucx.stream;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRequest;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@NotThreadSafe
public class JucxStreamSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private JucxStreamSynchronousChannel channel;
    private UcpMemory memory;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private long messageToWrite;
    private int position;
    private int remaining;
    private UcpRequest request;

    public JucxStreamSynchronousWriter(final JucxStreamSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        memory = channel.getUcpContext().memoryMap(channel.getUcpMemMapParams());
        buffer = new UnsafeByteBuffer(memory.getAddress(), channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, SocketSynchronousChannel.MESSAGE_INDEX);
    }

    @Override
    public void close() {
        if (buffer != null) {
            try {
                writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            memory.close();
            buffer = null;
            messageBuffer = null;
            messageToWrite = 0;
            position = 0;
            remaining = 0;
            request = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        try {
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(SocketSynchronousChannel.SIZE_INDEX, size);
            messageToWrite = buffer.addressOffset();
            position = 0;
            remaining = SocketSynchronousChannel.MESSAGE_INDEX + size;
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (messageToWrite == 0) {
            return true;
        } else if (!writeFurther()) {
            messageToWrite = 0;
            position = 0;
            remaining = 0;
            return true;
        } else {
            return false;
        }
    }

    private boolean writeFurther() throws IOException {
        if (request == null) {
            request = channel.getUcpEndpoint()
                    .sendStreamNonBlocking(buffer.addressOffset() + position, remaining,
                            channel.getErrorUcxCallback().reset());
        }
        try {
            channel.getUcpWorker().progressRequest(request);
        } catch (final Exception e) {
            throw new IOException(e);
        }
        channel.getErrorUcxCallback().maybeThrow();
        if (!request.isCompleted()) {
            return true;
        }
        remaining = 0;
        position += remaining;
        request = null;
        return false;
    }

}
