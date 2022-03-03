package de.invesdwin.context.integration.channel.sync.jnanomsg;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.jnanomsg.type.IJnanomsgSocketType;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;
import nanomsg.AbstractSocket;
import nanomsg.Nanomsg;
import nanomsg.NativeLibrary;

@NotThreadSafe
public class JnanomsgSynchronousReader extends AJnanomsgSynchronousChannel implements ISynchronousReader<IByteBuffer> {

    private final com.sun.jna.ptr.PointerByReference ptrBuff = new com.sun.jna.ptr.PointerByReference();
    private final UnsafeByteBuffer wrappedBuffer = new UnsafeByteBuffer();
    private IByteBuffer polledValue;
    private com.sun.jna.Pointer toBeFreedPointer;

    public JnanomsgSynchronousReader(final IJnanomsgSocketType socketType, final String addr, final boolean server) {
        super(socketType, addr, server);
    }

    @Override
    public void open() throws IOException {
        super.open();
    }

    @Override
    public void close() throws IOException {
        super.close();
        maybeFreePointer();
    }

    /**
     * https://github.com/niwinz/jnanomsg/pull/22
     */
    private void maybeFreePointer() {
        if (toBeFreedPointer != null) {
            NativeLibrary.nn_freemsg(toBeFreedPointer);
            toBeFreedPointer = null;
        }
    }

    @Override
    protected AbstractSocket newSocket(final IJnanomsgSocketType socketType) {
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
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer message = getPolledMessage();
        if (message != null && ClosedByteBuffer.isClosed(message)) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

    private IByteBuffer getPolledMessage() {
        if (polledValue != null) {
            final IByteBuffer value = polledValue;
            polledValue = null;
            return value;
        }
        try {
            return poll();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private IByteBuffer poll() throws IOException {
        maybeFreePointer();
        final int rc = NativeLibrary.nn_recv(socket.getFd(), ptrBuff, Nanomsg.NN_MSG, FLAGS_DONTWAIT);
        if (rc < 0) {
            final int errno = Nanomsg.getErrorNumber();
            if (errno == ERRORS_EAGAIN) {
                return null;
            }
            final String msg = Nanomsg.getError();
            close();
            throw new EOFException("closed by other side: [" + errno + "]=" + msg);
        } else {
            final com.sun.jna.Pointer toBeFreedPointer = ptrBuff.getValue();
            final long address = com.sun.jna.Pointer.nativeValue(toBeFreedPointer);
            final int offset = topic.length;
            final int length = rc - offset;
            wrappedBuffer.wrap(address + offset, length);
            return wrappedBuffer;
        }
    }

}
