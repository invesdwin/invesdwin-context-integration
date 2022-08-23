package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import de.invesdwin.util.bean.AValueObject;

/**
 * Adapted from io.netty.handler.ssl.SslUtils and io.netty.handler.ssl.JdkSslContext.Defaults
 * 
 * Can be used to print debug output for the SSLContext in the JDK.
 */
@NotThreadSafe
public class TlsDefaults extends AValueObject {

    private final String[] supportedProtocols;
    private final String[] enabledProtocols;
    private final String[] supportedCiphers;
    private final String[] enabledCiphers;
    private final Provider provider;

    public TlsDefaults(final ITlsProtocol protocol) {
        final SSLContext context;
        try {
            context = newContext(protocol);
            context.init(null, null, null);
        } catch (final Exception e) {
            throw new Error("failed to initialize the default SSL context", e);
        }

        provider = context.getProvider();

        final SSLEngine engine = context.createSSLEngine();
        supportedProtocols = engine.getSupportedProtocols();
        enabledProtocols = engine.getEnabledProtocols();

        supportedCiphers = engine.getSupportedCipherSuites();
        enabledCiphers = engine.getEnabledCipherSuites();
    }

    /**
     * Can use a different provider here.
     */
    protected SSLContext newContext(final ITlsProtocol protocol) throws NoSuchAlgorithmException {
        return SSLContext.getInstance(protocol.getFamily());
    }

    public String[] getSupportedProtocols() {
        return supportedProtocols;
    }

    public String[] getEnabledProtocols() {
        return enabledProtocols;
    }

    public String[] getEnabledCiphers() {
        return enabledCiphers;
    }

    public String[] getSupportedCiphers() {
        return supportedCiphers;
    }

    public Provider getProvider() {
        return provider;
    }

}
