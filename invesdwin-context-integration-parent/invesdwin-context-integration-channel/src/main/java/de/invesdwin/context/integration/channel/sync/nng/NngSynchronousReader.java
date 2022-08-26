package de.invesdwin.context.integration.channel.sync.nng;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.nng.type.INngSocketType;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.NioDelegateByteBuffer;
import io.sisu.nng.Message;
import io.sisu.nng.NngException;
import io.sisu.nng.Socket;

@NotThreadSafe
public class NngSynchronousReader extends ANngSynchronousChannel implements ISynchronousReader<IByteBufferProvider> {

    private final NioDelegateByteBuffer wrappedBuffer = new NioDelegateByteBuffer(EmptyByteBuffer.EMPTY_BYTE_BUFFER);
    private IByteBuffer polledValue;
    private Message messageToBeFreed;
    private java.nio.ByteBuffer body;

    public NngSynchronousReader(final INngSocketType socketType, final String addr, final boolean server) {
        super(socketType, addr, server);
    }

    @Override
    public void open() throws IOException {
        super.open();
    }

    @Override
    public void close() throws IOException {
        maybeFreeMessage();
        super.close();
    }

    @Override
    protected Socket newSocket(final INngSocketType socketType) throws NngException {
        return socketType.newReaderSocket();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (polledValue != null) {
            return true;
        }
        polledValue = poll();
        return polledValue != null;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final IByteBuffer message = getPolledMessage();
        if (message != null && ClosedByteBuffer.isClosed(message)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

    private IByteBuffer getPolledMessage() throws IOException {
        if (polledValue != null) {
            final IByteBuffer value = polledValue;
            polledValue = null;
            return value;
        }
        return poll();
    }

    private IByteBuffer poll() throws IOException {
        maybeFreeMessage();
        try {
            messageToBeFreed = socket.receiveMessage();
            if (messageToBeFreed == null) {
                return null;
            } else {
                final int bodyLen = messageToBeFreed.getBodyLen();
                body = messageToBeFreed.getBody();
                wrappedBuffer.setDelegate(body);
                return wrappedBuffer.slice(0, bodyLen);
            }
        } catch (final NngException e) {
            throw new IOException(e);
        }
    }

    private void maybeFreeMessage() {
        if (messageToBeFreed != null) {
            messageToBeFreed.free();
            body = null;
            messageToBeFreed = null;
        }
    }

}
