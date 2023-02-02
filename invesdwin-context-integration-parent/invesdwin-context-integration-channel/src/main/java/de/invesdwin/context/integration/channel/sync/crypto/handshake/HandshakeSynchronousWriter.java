package de.invesdwin.context.integration.channel.sync.crypto.handshake;

import java.io.IOException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class HandshakeSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final HandshakeChannel parent;

    private ISynchronousWriter<IByteBufferProvider> underlyingWriter;
    @GuardedBy("this during handshake")
    private volatile ISynchronousWriter<IByteBufferProvider> encryptedWriter;
    @GuardedBy("parent")
    private boolean readyForHandshake = false;

    public HandshakeSynchronousWriter(final HandshakeChannel parent) {
        this.parent = parent;
    }

    public ISynchronousWriter<IByteBufferProvider> getUnderlyingWriter() {
        return underlyingWriter;
    }

    public void setUnderlyingWriter(final ISynchronousWriter<IByteBufferProvider> underlyingWriter) {
        assert this.underlyingWriter == null : "Please always retrieve a reader/writer pair for the handshake to initialize properly. The writer was requested twice in a row which is unsupported.";
        this.underlyingWriter = underlyingWriter;
    }

    public synchronized ISynchronousWriter<IByteBufferProvider> getEncryptedWriter() {
        return encryptedWriter;
    }

    public synchronized void setEncryptedWriter(final ISynchronousWriter<IByteBufferProvider> encryptedWriter) {
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

    @Override
    public boolean writeReady() throws IOException {
        return encryptedWriter != null && encryptedWriter.writeReady();
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        encryptedWriter.write(message);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return encryptedWriter.writeFlushed();
    }

}