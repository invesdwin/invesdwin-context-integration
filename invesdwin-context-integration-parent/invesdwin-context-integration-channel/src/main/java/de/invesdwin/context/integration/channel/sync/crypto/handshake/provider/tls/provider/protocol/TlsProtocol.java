package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum TlsProtocol implements ITlsProtocol {
    /**
     * Requires a reliable underlying channel (e.g. TCP)
     */
    TLS {
        @Override
        public boolean isHandshakeTimeoutRecoveryEnabled() {
            return false;
        }
    },
    /**
     * For unreliable underlying channels (e.g. UDP)
     */
    DTLS {
        @Override
        public boolean isHandshakeTimeoutRecoveryEnabled() {
            return true;
        }
    };

    public static final TlsProtocol DEFAULT = TLS;

}
