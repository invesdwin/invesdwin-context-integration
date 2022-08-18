package de.invesdwin.context.integration.channel.sync.crypto.handshake;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.IHandshakeProvider;

@NotThreadSafe
public class HandshakeChannel {

    private final IHandshakeProvider parent;

    private final HandshakeReader reader = new HandshakeReader(this);
    private final HandshakeWriter writer = new HandshakeWriter(this);

    public HandshakeChannel(final IHandshakeProvider parent) {
        this.parent = parent;
    }

    public IHandshakeProvider getParent() {
        return parent;
    }

    public HandshakeReader getReader() {
        return reader;
    }

    public HandshakeWriter getWriter() {
        return writer;
    }

    public synchronized void open() throws IOException {
        if (reader.isReadyForHandshake() && writer.isReadyForHandshake()) {
            parent.handshake(this);
            reader.setReadyForHandshake(false);
            writer.setReadyForHandshake(false);
        }
    }

}