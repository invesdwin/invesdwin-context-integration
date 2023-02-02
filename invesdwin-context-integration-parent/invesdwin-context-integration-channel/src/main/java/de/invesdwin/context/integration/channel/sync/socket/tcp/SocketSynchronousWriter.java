package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class SocketSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private SocketSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private SocketChannel socketChannel;
    private java.nio.ByteBuffer messageToWrite;
    private int positionBefore;

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
                writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
            messageBuffer = null;
            socketChannel = null;
            messageToWrite = null;
            positionBefore = 0;
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

    /**
     * Old, blocking variation of the write
     */
    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        try {
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(SocketSynchronousChannel.SIZE_INDEX, size);
            messageToWrite = buffer.asNioByteBuffer(0, SocketSynchronousChannel.MESSAGE_INDEX + size);
            positionBefore = messageToWrite.position();
        } catch (final IOException e) {
            throw FastEOFException.getInstance(e);
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (messageToWrite == null) {
            return true;
        } else if (!writeFurther()) {
            ByteBuffers.position(messageToWrite, positionBefore);
            messageToWrite = null;
            positionBefore = 0;
            return true;
        } else {
            return false;
        }
    }

    private boolean writeFurther() throws IOException {
        final int count = socketChannel.write(messageToWrite);
        if (count < 0) { // EOF
            throw ByteBuffers.newEOF();
        }
        return messageToWrite.hasRemaining();
    }

}
