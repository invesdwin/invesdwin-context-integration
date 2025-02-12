package de.invesdwin.context.integration.channel.sync.socket.udp;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

@NotThreadSafe
public class DatagramSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private DatagramSynchronousChannel channel;
    private IByteBuffer buffer;
    private IByteBuffer messageBuffer;
    private DatagramChannel socketChannel;
    private final int socketSize;
    private java.nio.ByteBuffer messageToWrite;
    private int positionBefore;
    private SocketAddress recipient;

    public DatagramSynchronousWriter(final DatagramSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
        this.socketSize = channel.getSocketSize();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        //use direct buffer to prevent another copy from byte[] to native
        buffer = ByteBuffers.allocateDirectExpandable(socketSize);
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, DatagramSynchronousChannel.MESSAGE_INDEX);
        socketChannel = channel.getSocketChannel();
    }

    @Override
    public void close() throws IOException {
        final DatagramSynchronousChannel channelCopy = channel;
        if (buffer != null) {
            if (channelCopy != null) {
                if (!channelCopy.isServer() || !channelCopy.isMultipleClientsAllowed() && recipient != null) {
                    try {
                        writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
                    } catch (final Throwable t) {
                        //ignore
                    }
                }
            }
            buffer = null;
            messageBuffer = null;
            socketChannel = null;
            recipient = null;
        }
        if (channelCopy != null) {
            channelCopy.close();
            channel = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        if (channel.isServer()) {
            recipient = channel.getOtherSocketAddress();
        } else {
            recipient = channel.getSocketAddress();
        }
        final int size = message.getBuffer(messageBuffer);
        final int datagramSize = DatagramSynchronousChannel.MESSAGE_INDEX + size;
        if (datagramSize > socketSize) {
            throw new IllegalArgumentException(
                    "Data truncation would occur: datagramSize[" + datagramSize + "] > socketSize[" + socketSize + "]");
        }
        buffer.putInt(DatagramSynchronousChannel.SIZE_INDEX, size);
        messageToWrite = buffer.asNioByteBuffer(0, datagramSize);
        positionBefore = messageToWrite.position();
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
        final int count = socketChannel.send(messageToWrite, recipient);
        if (count < 0) { // EOF
            throw ByteBuffers.newEOF();
        }
        return messageToWrite.hasRemaining();
    }

}
