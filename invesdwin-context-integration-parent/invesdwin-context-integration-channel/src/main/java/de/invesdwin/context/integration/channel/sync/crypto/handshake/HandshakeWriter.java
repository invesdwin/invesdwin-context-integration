package de.invesdwin.context.integration.channel.sync.crypto.handshake;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class HandshakeWriter implements ISynchronousWriter<IByteBufferWriter> {

    private final HandshakeChannel parent;

    private ISynchronousWriter<IByteBufferWriter> underlyingWriter;
    @GuardedBy("this during handshake")
    private ISynchronousWriter<IByteBufferWriter> encryptedWriter;
    @GuardedBy("parent")
    private boolean readyForHandshake = false;

    public HandshakeWriter(final HandshakeChannel parent) {
        this.parent = parent;
    }

    public ISynchronousWriter<IByteBufferWriter> getUnderlyingWriter() {
        return underlyingWriter;
    }

    public void setUnderlyingWriter(final ISynchronousWriter<IByteBufferWriter> underlyingWriter) {
        assert this.underlyingWriter == null : "Please always retrieve a reader/writer pair for the handshake to initialize properly. The writer was requested twice in a row which is unsupported.";
        this.underlyingWriter = underlyingWriter;
    }

    public synchronized ISynchronousWriter<IByteBufferWriter> getEncryptedWriter() {
        return encryptedWriter;
    }

    public synchronized void setEncryptedWriter(final ISynchronousWriter<IByteBufferWriter> encryptedWriter) {
        this.encryptedWriter = encryptedWriter;
    }

    public boolean isReadyForHandshake() {
        synchronized (parent) {
            return readyForHandshake;
        }
    }

    public void setReadyForHandshake(final boolean readyForHandshake) {
        synchronized (parent) {
            this.readyForHandshake = readyForHandshake;
        }
    }

    @Override
    public void open() throws IOException {
        synchronized (parent) {
            underlyingWriter.open();
            readyForHandshake = true;
            //perform handshake on second open
            parent.open();
        }
    }

    @Override
    public void close() throws IOException {
        if (encryptedWriter != null) {
            encryptedWriter.close();
            encryptedWriter = null;
        }
        if (underlyingWriter != null) {
            underlyingWriter.close();
        }
    }

    /**
     * Override this to disable spinning or configure type of waits.
     */
    protected ASpinWait newSpinWait() {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                //using synchronized getter so we don't need to make it volatile
                return getEncryptedWriter() != null;
            }
        };
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        if (encryptedWriter == null) {
            //wait for handshake
            try {
                final Duration handshakeTimeout = parent.getParent().getHandshakeTimeout();
                if (!newSpinWait().awaitFulfill(System.nanoTime(), handshakeTimeout)) {
                    throw new TimeoutException("Read handshake message timeout exceeded: " + handshakeTimeout);
                }
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
        encryptedWriter.write(message);
    }

}