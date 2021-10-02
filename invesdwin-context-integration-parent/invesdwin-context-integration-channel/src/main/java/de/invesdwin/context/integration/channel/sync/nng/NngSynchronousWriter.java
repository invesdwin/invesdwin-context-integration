package de.invesdwin.context.integration.channel.sync.nng;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.nng.type.INngSocketType;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import io.sisu.nng.NngException;
import io.sisu.nng.Socket;

@NotThreadSafe
public class NngSynchronousWriter extends ANngSynchronousChannel implements ISynchronousWriter<IByteBufferWriter> {

    private IByteBuffer buffer;
    private IByteBuffer messageBuffer;

    public NngSynchronousWriter(final INngSocketType socketType, final String addr, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketType, addr, server, estimatedMaxMessageSize);
    }

    @Override
    protected Socket newSocket(final INngSocketType socketType) throws NngException {
        return socketType.newWriterSocket();
    }

    @Override
    public void open() throws IOException {
        super.open();
        buffer = ByteBuffers.allocateDirectExpandable();
        if (topic.length > 0) {
            buffer.putBytes(0, topic);
        }
        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, messageIndex);
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        try {
            final int size = message.write(messageBuffer);
            buffer.putInt(sizeIndex, size);
            socket.sendMessage(buffer.asNioByteBuffer(0, messageIndex + size));
        } catch (final NngException e) {
            throw new IOException(e);
        }
    }

}
