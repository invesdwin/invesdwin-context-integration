package de.invesdwin.context.integration.channel.sync.crypto.handshake;

import java.io.IOException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class HandshakeReader implements ISynchronousReader<IByteBuffer> {

    private final HandshakeChannel parent;

    private ISynchronousReader<IByteBuffer> underlyingReader;
    @GuardedBy("this during handshake")
    private ISynchronousReader<IByteBuffer> encryptedReader;
    @GuardedBy("parent")
    private boolean readyForHandshake = false;

    public HandshakeReader(final HandshakeChannel parent) {
        this.parent = parent;
    }

    public ISynchronousReader<IByteBuffer> getUnderlyingReader() {
        return underlyingReader;
    }

    public void setUnderlyingReader(final ISynchronousReader<IByteBuffer> underlyingReader) {
        assert this.underlyingReader == null : "Please always retrieve a reader/writer pair for the handshake to initialize properly. The reader was requested twice in a row which is unsupported.";
        this.underlyingReader = underlyingReader;
    }

    public synchronized ISynchronousReader<IByteBuffer> getEncryptedReader() {
        return encryptedReader;
    }

    public synchronized void setEncryptedReader(final ISynchronousReader<IByteBuffer> encryptedReader) {
        this.encryptedReader = encryptedReader;
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

    /**
     * open needs to be threadsafe
     */
    @Override
    public void open() throws IOException {
        synchronized (parent) {
            underlyingReader.open();
            readyForHandshake = true;
            //perform handshake on second open
            parent.open();
        }
    }

    @Override
    public void close() throws IOException {
        if (encryptedReader != null) {
            encryptedReader.close();
            encryptedReader = null;
        }
        underlyingReader.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (encryptedReader == null) {
            //using synchronized getter so we don't need to make it volatile
            if (getEncryptedReader() == null) {
                //wait for handshake
                return false;
            }
        }
        return encryptedReader.hasNext();
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        return encryptedReader.readMessage();
    }

    @Override
    public void readFinished() {
        encryptedReader.readFinished();
    }

}