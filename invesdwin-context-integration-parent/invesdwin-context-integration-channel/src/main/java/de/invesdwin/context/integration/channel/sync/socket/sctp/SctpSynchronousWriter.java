package de.invesdwin.context.integration.channel.sync.socket.sctp;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class SctpSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private SctpSynchronousChannel channel;
    private IByteBuffer buffer;
    private SlicedFromDelegateByteBuffer messageBuffer;
    private Object socketChannel;
    private Object outMessageInfo;
    private java.nio.ByteBuffer messageToWrite;
    private int positionBefore;

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
            messageToWrite = null;
            positionBefore = 0;
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
            messageToWrite = buffer.asNioByteBuffer(0, SctpSynchronousChannel.MESSAGE_INDEX + size);
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
        write0(socketChannel, messageToWrite, outMessageInfo);
        return messageToWrite.hasRemaining();
    }

    private static int write0(final Object dst, final java.nio.ByteBuffer byteBuffer, final Object outMessageInfo)
            throws IOException {
        final int count;
        try {
            count = (int) SctpSynchronousChannel.SCTPCHANNEL_SEND_METHOD.invoke(dst, byteBuffer, outMessageInfo);
        } catch (final IOException e) {
            throw e;
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
        if (count < 0) { // EOF
            throw ByteBuffers.newEOF();
        }
        return count;
    }

}
