package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.annotation.concurrent.Immutable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.operator.OperatorCreationException;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.IDerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.certificate.SelfSignedCertGenerator;
import de.invesdwin.context.security.crypto.verification.signature.SignatureKey;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.EcdsaAlgorithm;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.ISignatureAlgorithm;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.reflection.field.UnsafeField;
import de.invesdwin.util.time.range.TimeRange;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

/**
 * WARNING: With this implementation the confidentiality relies solely on the CryptoProperties.DEFAULT_PEPPER. It would
 * be advisable to use a pre shared certificate so that the client does not know how to generate the private key.
 * Especially when the process runs on untrusted client hardware. So override this implementation to do something more
 * secure.
 */
@Immutable
public class DerivedKeyTransportLayerSecurityProvider implements ITransportLayerSecurityProvider {

    private static final UnsafeField<String[]> FIELD_PROTOCOLS;
    private static final UnsafeField<String[]> FIELD_CIPHERSUITES;

    static {
        try {
            final Field protocolsField = JdkSslContext.class.getDeclaredField("protocols");
            FIELD_PROTOCOLS = new UnsafeField<>(protocolsField);
            final Field cipherSuitesField = JdkSslContext.class.getDeclaredField("cipherSuites");
            FIELD_CIPHERSUITES = new UnsafeField<>(cipherSuitesField);
        } catch (NoSuchFieldException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private final InetSocketAddress socketAddress;
    private final boolean server;

    public DerivedKeyTransportLayerSecurityProvider(final InetSocketAddress socketAddress, final boolean server) {
        this.socketAddress = socketAddress;
        this.server = server;
    }

    public boolean isServer() {
        return server;
    }

    /**
     * see io.netty.handler.ssl.JdkSslContext.configureAndWrapEngine(SSLEngine, ByteBufAllocator)
     */
    @Override
    public void configureServerSocket(final SSLServerSocket socket) {
        final JdkSslContext context = (JdkSslContext) newNettyContext(SslProvider.JDK);

        final String[] cipherSuites = FIELD_CIPHERSUITES.get(context);
        final String[] protocols = FIELD_PROTOCOLS.get(context);

        socket.setEnabledCipherSuites(cipherSuites);
        socket.setEnabledProtocols(protocols);
        socket.setUseClientMode(!isServer());
        if (isServer()) {
            switch (getClientAuth()) {
            case OPTIONAL:
                socket.setWantClientAuth(true);
                break;
            case REQUIRE:
                socket.setNeedClientAuth(true);
                break;
            case NONE:
                break;
            default:
                throw UnknownArgumentException.newInstance(ClientAuth.class, getClientAuth());
            }
        }
    }

    /**
     * see io.netty.handler.ssl.JdkSslContext.configureAndWrapEngine(SSLEngine, ByteBufAllocator)
     */
    @Override
    public void configureSocket(final SSLSocket socket) {
        final JdkSslContext context = (JdkSslContext) newNettyContext(SslProvider.JDK);

        final String[] cipherSuites = FIELD_CIPHERSUITES.get(context);
        final String[] protocols = FIELD_PROTOCOLS.get(context);

        socket.setEnabledCipherSuites(cipherSuites);
        socket.setEnabledProtocols(protocols);
        socket.setUseClientMode(!isServer());
        if (isServer()) {
            switch (getClientAuth()) {
            case OPTIONAL:
                socket.setWantClientAuth(true);
                break;
            case REQUIRE:
                socket.setNeedClientAuth(true);
                break;
            case NONE:
                break;
            default:
                throw UnknownArgumentException.newInstance(ClientAuth.class, getClientAuth());
            }
        }
    }

    @Override
    public void onSocketConnected(final SSLSocket socket) {
        //        try {
        //            socket.startHandshake();
        //        } catch (final IOException e) {
        //            throw new RuntimeException(e);
        //        }
    }

    @Override
    public SSLContext newContext() {
        final JdkSslContext nettyContext = (JdkSslContext) newNettyContext(SslProvider.JDK);
        final SSLContext context = nettyContext.context();
        return context;
    }

    @Override
    public SSLEngine newEngine() {
        final SslContext nettyContext = newNettyContext(getEngineSslProvider());
        if (server) {
            return nettyContext.newEngine(getByteBufAllocator());
        } else {
            return nettyContext.newEngine(getByteBufAllocator(), socketAddress.getHostName(), socketAddress.getPort());
        }
    }

    protected ByteBufAllocator getByteBufAllocator() {
        return ByteBufAllocator.DEFAULT;
    }

    protected SslContext newNettyContext(final SslProvider provider) {
        final ISignatureAlgorithm signatureAlgorithm = getSignatureAlgorithm();
        final ClientAuth clientAuth = getClientAuth();
        final boolean startTls = isStartTlsEnabled();
        final boolean mTls = clientAuth != ClientAuth.NONE;

        final IDerivedKeyProvider derivedKeyProvider = DerivedKeyProvider.fromPassword(getDerivedKeyPepper(),
                getDerivedKeyPassword());
        final SignatureKey signatureKey = derivedKeyProvider.newDerivedKey(signatureAlgorithm, getDerivedKeyInfo(),
                signatureAlgorithm.getDefaultKeySizeBits());
        final KeyPair keyPair = new KeyPair(signatureKey.getVerifyKey(), signatureKey.getSignKey());
        try {
            final TimeRange validity = SelfSignedCertGenerator.newBrowserValidity();
            final String hostname = getHostname();
            final X509Certificate serverCertificate = SelfSignedCertGenerator.generate(keyPair,
                    signatureAlgorithm.getAlgorithm(), hostname, validity);
            if (server) {
                final SslContextBuilder forServer = SslContextBuilder.forServer(keyPair.getPrivate(),
                        serverCertificate);
                if (mTls) {
                    forServer.trustManager(serverCertificate);
                }
                return forServer.sslProvider(provider).clientAuth(clientAuth).startTls(startTls).build();
            } else {
                final SslContextBuilder forClient = SslContextBuilder.forClient();
                if (mTls) {
                    forClient.keyManager(keyPair.getPrivate(), serverCertificate);
                }
                return forClient.trustManager(serverCertificate)
                        .sslProvider(provider)
                        .clientAuth(clientAuth)
                        .startTls(startTls)
                        .build();
            }
        } catch (OperatorCreationException | CertificateException | CertIOException | SSLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isStartTlsEnabled() {
        return CryptoProperties.DEFAULT_START_TLS_ENABLED;
    }

    protected byte[] getDerivedKeyPepper() {
        return CryptoProperties.DEFAULT_PEPPER;
    }

    protected byte[] getDerivedKeyPassword() {
        return "netty-ssl-engine-password".getBytes();
    }

    protected byte[] getDerivedKeyInfo() {
        return "netty-ssl-engine-key".getBytes();
    }

    protected ISignatureAlgorithm getSignatureAlgorithm() {
        //netty does not support EdDSA: https://github.com/netty/netty/issues/10916
        return EcdsaAlgorithm.DEFAULT;
        //netty-tcnative-boringssl-static does not support EcDSA
        //        switch (sslProvider) {
        //        case JDK:
        //            signatureAlgorithm = EcdsaAlgorithm.DEFAULT;
        //            break;
        //        case OPENSSL:
        //        case OPENSSL_REFCNT:
        //            signatureAlgorithm = RsaSignatureAlgorithm.DEFAULT;
        //            break;
        //        default:
        //            throw UnknownArgumentException.newInstance(SslProvider.class, sslProvider);
        //        }
    }

    protected ClientAuth getClientAuth() {
        //we use mTls per default
        return ClientAuth.REQUIRE;
    }

    protected String getHostname() {
        if (server) {
            return IntegrationProperties.HOSTNAME;
        } else {
            return socketAddress.getHostName();
        }
    }

    protected SslProvider getEngineSslProvider() {
        if (server) {
            return SslContext.defaultServerProvider();
        } else {
            return SslContext.defaultClientProvider();
        }
    }

}
