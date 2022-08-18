package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.ecdh;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.IHandshakeProvider;
import de.invesdwin.util.time.duration.Duration;

/**
 * In its default configuration, this is actually an authenticated (with pre shared password+pepper based encryption),
 * then signed (with random+pepper ephemeral signature) ephemeral elliptic curve diffie hellman handshake (authenticated
 * and signed ECDHE).
 */
@Immutable
public class SignedEcdhHandshakeProvider implements IHandshakeProvider {

    private final IHandshakeProvider delegate;

    public SignedEcdhHandshakeProvider(final Duration handshakeTimeout) {
        this.delegate = newDelegate(handshakeTimeout);
    }

    protected IHandshakeProvider newDelegate(final Duration handshakeTimeout) {
        return new EcdhHandshakeProvider(handshakeTimeout).asSigned();
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
