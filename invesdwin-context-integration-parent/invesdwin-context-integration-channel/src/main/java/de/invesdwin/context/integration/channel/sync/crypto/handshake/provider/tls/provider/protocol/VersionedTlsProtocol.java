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
    SSL_v2_HELLO(TlsProtocol.SSL, "SSLv2Hello"),

    /**
     * SSL v2
     *
     * @deprecated SSLv2 is no longer secure. Consider using {@link #TLS_v1_2} or {@link #TLS_v1_3}
     */
    @Deprecated
    SSL_v2(TlsProtocol.SSL, "SSLv2"),

    /**
     * SSLv3
     *
     * @deprecated SSLv3 is no longer secure. Consider using {@link #TLS_v1_2} or {@link #TLS_v1_3}
     */
    @Deprecated
    SSL_v3(TlsProtocol.SSL, "SSLv3"),

    /**
     * TLS v1
     *
     * @deprecated TLSv1 is no longer secure. Consider using {@link #TLS_v1_2} or {@link #TLS_v1_3}
     */
    @Deprecated
    TLS_v1(TlsProtocol.TLS, "TLSv1"),

    /**
     * TLS v1.1
     *
     * @deprecated TLSv1.1 is no longer secure. Consider using {@link #TLS_v1_2} or {@link #TLS_v1_3}
     */
    @Deprecated
    TLS_v1_1(TlsProtocol.TLS, "TLSv1.1"),

    /**
     * TLS v1.2
     */
    TLS_v1_2(TlsProtocol.TLS, "TLSv1.2"),

    /**
     * TLS v1.3
     */
    TLS_v1_3(TlsProtocol.TLS, "TLSv1.3"),
    /**
     * DTLS v1.0
     */
    DTLS_v1_0(TlsProtocol.DTLS, "DTLSv1.0"),
    /**
     * DTLS v1.3
     */
    DTLS_v1_3(TlsProtocol.DTLS, "DTLSv1.3");

    private ITlsProtocol family;
    private String name;

    VersionedTlsProtocol(final ITlsProtocol family, final String name) {
        this.family = family;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getFamily() {
        return family.getFamily();
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public boolean isHandshakeTimeoutRecoveryEnabled() {
        return family.isHandshakeTimeoutRecoveryEnabled();
    }

    @Override
    public boolean isVersioned() {
        return true;
    }

}
