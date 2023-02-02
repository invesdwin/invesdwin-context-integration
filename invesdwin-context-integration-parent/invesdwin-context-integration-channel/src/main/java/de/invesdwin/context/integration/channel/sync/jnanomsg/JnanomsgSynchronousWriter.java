package de.invesdwin.context.integration.channel.sync.jnanomsg;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jnanomsg.type.IJnanomsgSocketType;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import nanomsg.AbstractSocket;
import nanomsg.Nanomsg;
import nanomsg.NativeLibrary;

@NotThreadSafe
public class JnanomsgSynchronousWriter extends AJnanomsgSynchronousChannel
        implements ISynchronousWriter<IByteBufferProvider> {

    private IByteBuffer buffer;
    private IByteBuffer messageBuffer;
    private int length;

    public JnanomsgSynchronousWriter(final IJnanomsgSocketType socketType, final String addr, final boolean server) {
        super(socketType, addr, server);
    }

    @Override
    protected AbstractSocket newSocket(final IJnanomsgSocketType socketType) {
        return socketType.newWriterSocket();
    }

    @Override
    public void open() throws IOException {
        super.open();
        buffer = ByteBuffers.allocateDirectExpandable();
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

    private boolean sendTry(final int size) throws IOException {
        final int rc = NativeLibrary.nn_send(socket.getFd(), buffer.nioByteBuffer(), size, FLAGS_DONTWAIT);

        if (rc < 0) {
            final int errno = Nanomsg.getErrorNumber();
            if (errno == ERRORS_EAGAIN) {
                return false;
            }
            final String msg = Nanomsg.getError();
            close();
            throw FastEOFException.getInstance("closed by other side: [" + errno + "]=" + msg);
        } else {
            return true;
        }
    }

}
