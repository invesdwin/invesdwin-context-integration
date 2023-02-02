package de.invesdwin.context.integration.channel.sync.jeromq;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.api.MessageFlag;
import org.zeromq.api.SocketType;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jeromq.type.IJeromqSocketType;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.extend.ArrayExpandableByteBuffer;
import zmq.ZError;

@NotThreadSafe
public class JeromqSynchronousWriter extends AJeromqSynchronousChannel
        implements ISynchronousWriter<IByteBufferProvider> {

    private ArrayExpandableByteBuffer buffer;
    private IByteBuffer messageBuffer;
    private int length;

    public JeromqSynchronousWriter(final IJeromqSocketType socketType, final String addr, final boolean server) {
        this(socketType.getWriterSocketType(), addr, server);
    }

    public JeromqSynchronousWriter(final SocketType socketType, final String addr, final boolean server) {
        super(socketType, addr, server);
    }

    @Override
    public void open() throws IOException {
        super.open();
        buffer = new ArrayExpandableByteBuffer();
        if (topic.length > 0) {
            buffer.putBytes(0, topic);
            messageBuffer = new SlicedFromDelegateByteBuffer(buffer, topic.length);
        } else {
            messageBuffer = buffer;
        }
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            try {
                writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
            length = 0;
        }
        super.close();
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int size = message.getBuffer(messageBuffer);
        this.length = messageIndex + size;
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (length == 0) {
            return true;
        } else if (sendTry(length)) {
            length = 0;
            return true;
        } else {
            return false;
        }
    }

    private boolean sendTry(final int size) throws IOException, EOFException {
        final boolean sent = socket.send(buffer.byteArray(), 0, size, MessageFlag.DONT_WAIT);
        if (!sent) {
            if (socket.getZMQSocket().errno() == ZError.EAGAIN) {
                //backpressure
                return false;
            }
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return true;
    }

}
