package de.invesdwin.context.integration.channel.sync.jnanomsg;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jnanomsg.type.IJnanomsgSocketType;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.time.date.FTimeUnit;
import nanomsg.AbstractSocket;
import nanomsg.Nanomsg;
import nanomsg.NativeLibrary;

@NotThreadSafe
public class JnanomsgSynchronousWriter extends AJnanomsgSynchronousChannel
        implements ISynchronousWriter<IByteBufferProvider> {

    private IByteBuffer buffer;
    private IByteBuffer messageBuffer;

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
                writeNoRetry(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            buffer = null;
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
        sendRetrying(size + messageIndex);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

    private void writeNoRetry(final IByteBufferProvider message) throws IOException {
        final int size = message.getBuffer(messageBuffer);
        sendTry(size + messageIndex);
    }

    private void sendRetrying(final int size) throws IOException, EOFException, InterruptedIOException {
        while (!sendTry(size)) {
            try {
                FTimeUnit.MILLISECONDS.sleep(1);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                final InterruptedIOException interrupt = new InterruptedIOException(e.getMessage());
                interrupt.initCause(e);
                throw interrupt;
            }
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
