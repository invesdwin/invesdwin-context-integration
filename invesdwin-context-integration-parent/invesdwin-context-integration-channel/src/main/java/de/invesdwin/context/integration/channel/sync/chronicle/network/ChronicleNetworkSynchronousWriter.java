package de.invesdwin.context.integration.channel.sync.chronicle.network;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

@NotThreadSafe
public class ChronicleNetworkSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private ChronicleNetworkSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private ChronicleSocketChannel socketChannel;
    private java.nio.ByteBuffer messageToWrite;
    private int positionBefore;

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

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        try {
            final int size = message.getBuffer(messageBuffer);
            buffer.putInt(ChronicleNetworkSynchronousChannel.SIZE_INDEX, size);
            messageToWrite = buffer.asNioByteBuffer(0, ChronicleNetworkSynchronousChannel.MESSAGE_INDEX + size);
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
