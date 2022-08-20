package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class SocketSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

    private final SocketSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private SocketChannel socketChannel;

    public SocketSynchronousWriter(final SocketSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        if (!channel.isWriterRegistered()) {
            if (channel.getSocket() != null) {
                channel.getSocket().shutdownInput();
            }
        }
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, SocketSynchronousChannel.MESSAGE_INDEX);
        socketChannel = channel.getSocketChannel();
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
            channel.close();
        }
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        try {
            final int size = message.writeBuffer(messageBuffer);
            buffer.putInt(SocketSynchronousChannel.SIZE_INDEX, size);
            buffer.getBytesTo(0, socketChannel, SocketSynchronousChannel.MESSAGE_INDEX + size);
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

}
