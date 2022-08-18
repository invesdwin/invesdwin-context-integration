package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider;

import java.io.IOException;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.ecdh.SignedEcdhHandshakeProvider;
import de.invesdwin.util.time.duration.Duration;

public interface IHandshakeProvider {

    static IHandshakeProvider getDefault() {
        return getDefault(ContextProperties.DEFAULT_NETWORK_TIMEOUT);
    }

    static IHandshakeProvider getDefault(final Duration handshakeTimeout) {
        return new SignedEcdhHandshakeProvider(handshakeTimeout);
    }

    void handshake(HandshakeChannel channel) throws IOException;

    Duration getHandshakeTimeout();

}
