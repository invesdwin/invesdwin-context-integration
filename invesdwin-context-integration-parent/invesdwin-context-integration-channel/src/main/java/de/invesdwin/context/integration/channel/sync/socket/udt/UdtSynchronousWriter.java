package de.invesdwin.context.integration.channel.sync.socket.udt;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.barchart.udt.SocketUDT;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class UdtSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private UdtSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private SocketUDT socket;
    private java.nio.ByteBuffer messageToWrite;
    private int positionBefore;

    public UdtSynchronousWriter(final UdtSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(channel.getSocketSize());
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, UdtSynchronousChannel.MESSAGE_INDEX);
        socket = channel.getSocket();
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
            socket = null;
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
            buffer.putInt(UdtSynchronousChannel.SIZE_INDEX, size);
            messageToWrite = buffer.asNioByteBuffer(0, UdtSynchronousChannel.MESSAGE_INDEX + size);
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
        final int count = socket.send(messageToWrite);
        if (count < 0) { // EOF
            throw ByteBuffers.newEOF();
        }
        return messageToWrite.hasRemaining();
    }

}
