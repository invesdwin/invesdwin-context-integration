package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import de.invesdwin.util.bean.AValueObject;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.Collections;

/**
 * Adapted from io.netty.handler.ssl.SslUtils and io.netty.handler.ssl.JdkSslContext.Defaults
 * 
 * Can be used to print debug output for the SSLContext in the JDK.
 */
@NotThreadSafe
public class TlsDefaults extends AValueObject {

    private final String[] supportedProtocols;
    private final String[] defaultProtocols;
    private final List<String> defaultCiphers;
    private final Set<String> supportedCiphers;
    private final Provider defaultProvider;

    public TlsDefaults(final ITlsProtocol protocol) {
        final SSLContext context;
        try {
            context = newContext(protocol);
            context.init(null, null, null);
        } catch (final Exception e) {
            throw new Error("failed to initialize the default SSL context", e);
        }

        defaultProvider = context.getProvider();

        final SSLEngine engine = context.createSSLEngine();
        supportedProtocols = context.getDefaultSSLParameters().getProtocols();
        defaultProtocols = engine.getEnabledProtocols();

        supportedCiphers = Collections.unmodifiableSet(supportedCiphers(engine));
        defaultCiphers = Collections.unmodifiableList(defaultCiphers(engine, supportedCiphers));
    }

    /**
     * Can use a different provider here.
     */
    protected SSLContext newContext(final ITlsProtocol protocol) throws NoSuchAlgorithmException {
        return SSLContext.getInstance(protocol.getName());
    }

    private static Set<String> supportedCiphers(final SSLEngine engine) {
        // Choose the sensible default list of cipher suites.
        final String[] supportedCiphers = engine.getSupportedCipherSuites();
        final Set<String> supportedCiphersSet = new LinkedHashSet<String>(supportedCiphers.length);
        for (int i = 0; i < supportedCiphers.length; ++i) {
            final String supportedCipher = supportedCiphers[i];
            supportedCiphersSet.add(supportedCipher);
            // IBM's J9 JVM utilizes a custom naming scheme for ciphers and only returns ciphers with the "SSL_"
            // prefix instead of the "TLS_" prefix (as defined in the JSSE cipher suite names [1]). According to IBM's
            // documentation [2] the "SSL_" prefix is "interchangeable" with the "TLS_" prefix.
            // See the IBM forum discussion [3] and issue on IBM's JVM [4] for more details.
            //[1] https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites
            //[2] https://www.ibm.com/support/knowledgecenter/en/SSYKE2_8.0.0/com.ibm.java.security.component.80.doc/
            // security-component/jsse2Docs/ciphersuites.html
            //[3] https://www.ibm.com/developerworks/community/forums/html/topic?id=9b5a56a9-fa46-4031-b33b-df91e28d77c2
            //[4] https://www.ibm.com/developerworks/rfe/execute?use_case=viewRfe&CR_ID=71770
            if (supportedCipher.startsWith("SSL_")) {
                final String tlsPrefixedCipherName = "TLS_" + supportedCipher.substring("SSL_".length());
                try {
                    engine.setEnabledCipherSuites(new String[] { tlsPrefixedCipherName });
                    supportedCiphersSet.add(tlsPrefixedCipherName);
                } catch (final IllegalArgumentException ignored) {
                    // The cipher is not supported ... move on to the next cipher.
                }
            }
        }
        return supportedCiphersSet;
    }

    private static List<String> defaultCiphers(final SSLEngine engine, final Set<String> supportedCiphers) {
        final List<String> ciphers = new ArrayList<String>();
        useFallbackCiphersIfDefaultIsEmpty(ciphers, engine.getEnabledCipherSuites());
        return ciphers;
    }

    private static void useFallbackCiphersIfDefaultIsEmpty(final List<String> defaultCiphers,
            final Iterable<String> fallbackCiphers) {
        if (defaultCiphers.isEmpty()) {
            for (final String cipher : fallbackCiphers) {
                if (cipher.startsWith("SSL_") || cipher.contains("_RC4_")) {
                    continue;
                }
                defaultCiphers.add(cipher);
            }
        }
    }

    private static void useFallbackCiphersIfDefaultIsEmpty(final List<String> defaultCiphers,
            final String... fallbackCiphers) {
        useFallbackCiphersIfDefaultIsEmpty(defaultCiphers, Arrays.asList(fallbackCiphers));
    }

    public String[] getSupportedProtocols() {
        return supportedProtocols;
    }

    public String[] getDefaultProtocols() {
        return defaultProtocols;
    }

    public List<String> getDefaultCiphers() {
        return defaultCiphers;
    }

    public Set<String> getSupportedCiphers() {
        return supportedCiphers;
    }

    public Provider getDefaultProvider() {
        return defaultProvider;
    }

}
