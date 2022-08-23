package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum TlsProtocol implements ITlsProtocol {
    /**
     * TLS builds on top of and replaces this old specification. On newer JVMs this is treated as an alias for TLS.
     */
    @Deprecated
    SSL {
        @Override
        public boolean isHandshakeTimeoutRecoveryEnabled() {
            return false;
        }
    },
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
     * 
     * Supports order protection/non-replayability during handshake.
     */
    DTLS {
        @Override
        public boolean isHandshakeTimeoutRecoveryEnabled() {
            return true;
        }
    };

    public static final TlsProtocol DEFAULT = TLS;

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String getFamily() {
        return name();
    }

    @Override
    public boolean isVersioned() {
        return false;
    }

}
