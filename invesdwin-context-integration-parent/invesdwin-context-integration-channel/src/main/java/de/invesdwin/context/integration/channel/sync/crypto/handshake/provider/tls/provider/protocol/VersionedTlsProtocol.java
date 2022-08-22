package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum VersionedTlsProtocol implements ITlsProtocol {

    /**
     * SSL v2 Hello
     *
     * @deprecated SSLv2Hello is no longer secure. Consider using {@link #TLS_v1_2} or {@link #TLS_v1_3}
     */
    @Deprecated
    SSL_v2_HELLO("SSLv2Hello", false),

    /**
     * SSL v2
     *
     * @deprecated SSLv2 is no longer secure. Consider using {@link #TLS_v1_2} or {@link #TLS_v1_3}
     */
    @Deprecated
    SSL_v2("SSLv2", false),

    /**
     * SSLv3
     *
     * @deprecated SSLv3 is no longer secure. Consider using {@link #TLS_v1_2} or {@link #TLS_v1_3}
     */
    @Deprecated
    SSL_v3("SSLv3", false),

    /**
     * TLS v1
     *
     * @deprecated TLSv1 is no longer secure. Consider using {@link #TLS_v1_2} or {@link #TLS_v1_3}
     */
    @Deprecated
    TLS_v1("TLSv1", false),

    /**
     * TLS v1.1
     *
     * @deprecated TLSv1.1 is no longer secure. Consider using {@link #TLS_v1_2} or {@link #TLS_v1_3}
     */
    @Deprecated
    TLS_v1_1("TLSv1.1", false),

    /**
     * TLS v1.2
     */
    TLS_v1_2("TLSv1.2", false),

    /**
     * TLS v1.3
     */
    TLS_v1_3("TLSv1.3", false),
    /**
     * DTLS v1.0
     *
     * @deprecated DTLSv1.0 is no longer secure. Consider using {@link #DTLS_v1_3}
     */
    @Deprecated
    DTLS_v1_0("DTLSv1.0", true),
    /**
     * DTLS v1.3
     */
    DTLS_v1_3("DTLSv1.3", true);

    private String name;
    private boolean recovery;

    VersionedTlsProtocol(final String name, final boolean recovery) {
        this.name = name;
        this.recovery = recovery;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public boolean isHandshakeTimeoutRecoveryEnabled() {
        return recovery;
    }

}
