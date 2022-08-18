package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.dh;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.IHandshakeProvider;
import de.invesdwin.util.time.duration.Duration;

/**
 * In its default configuration, this is actually an authenticated (with pre shared password+pepper based encryption),
 * then signed (with random+pepper ephemeral signature) ephemeral diffie hellman handshake (athenticated and signed
 * EDH/DHE).
 * 
 * WARNING: Prefer the SignedEcdhHandshakeProvider where possible.
 */
@Immutable
public class SignedDhHandshakeProvider implements IHandshakeProvider {

    private final IHandshakeProvider delegate;

    public SignedDhHandshakeProvider(final Duration handshakeTimeout) {
        this.delegate = newDelegate(handshakeTimeout);
    }

    protected IHandshakeProvider newDelegate(final Duration handshakeTimeout) {
        return new DhHandshakeProvider(handshakeTimeout).asSigned();
    }

    @Override
    public void handshake(final HandshakeChannel channel) throws IOException {
        delegate.handshake(channel);
    }

    @Override
    public Duration getHandshakeTimeout() {
        return delegate.getHandshakeTimeout();
    }

}
