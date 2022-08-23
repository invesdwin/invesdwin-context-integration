package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

import javax.annotation.concurrent.Immutable;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.ClientAuth;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.ITlsProtocol;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.TlsProtocol;
import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.IDerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.certificate.KeyStores;
import de.invesdwin.context.security.crypto.key.certificate.SelfSignedCertGenerator;
import de.invesdwin.context.security.crypto.random.CryptoRandomGenerators;
import de.invesdwin.context.security.crypto.verification.signature.SignatureKey;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.EcdsaAlgorithm;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.ISignatureAlgorithm;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.time.range.TimeRange;

/**
 * WARNING: With this implementation the confidentiality relies solely on the CryptoProperties.DEFAULT_PEPPER. It would
 * be advisable to use a pre shared certificate so that the client does not know how to generate the private key.
 * Especially when the process runs on untrusted client hardware. So override this implementation to do something more
 * secure.
 */
@Immutable
public class DerivedKeyTransportLayerSecurityProvider implements ITransportLayerSecurityProvider {

    /**
     * JDK does not support EdDSA currently for TLS
     * 
     * Netty does not support EdDSA: https://github.com/netty/netty/issues/10916
     */
    public static final EcdsaAlgorithm DEFAULT_SIGNATURE_ALGORITHM = EcdsaAlgorithm.DEFAULT;
    /**
     * We use mTls per default
     */
    public static final ClientAuth DEFAULT_CLIENT_AUTH = ClientAuth.NEED;
    public static final String DEFAULT_DERIVED_KEY_PASSWORD = "ssl-engine-password";
    public static final String DEFAULT_DERIVED_KEY_INFO = "ssl-engine-key";
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
        socket.setUseClientMode(!isServer());
        if (isServer()) {
            switch (getClientAuth()) {
            case NEED:
                socket.setNeedClientAuth(true);
                break;
            case WANT:
                socket.setWantClientAuth(true);
                break;
            case NONE:
                break;
            default:
                throw UnknownArgumentException.newInstance(ClientAuth.class, getClientAuth());
            }
        }
        final SSLParameters params = socket.getSSLParameters();
        configureParams(params);
        socket.setSSLParameters(params);
    }

    /**
     * see io.netty.handler.ssl.JdkSslContext.configureAndWrapEngine(SSLEngine, ByteBufAllocator)
     */
    @Override
    public void configureSocket(final SSLSocket socket) {
        socket.setUseClientMode(!isServer());
        if (isServer()) {
            switch (getClientAuth()) {
            case NEED:
                socket.setNeedClientAuth(true);
                break;
            case WANT:
                socket.setWantClientAuth(true);
                break;
            case NONE:
                break;
            default:
                throw UnknownArgumentException.newInstance(ClientAuth.class, getClientAuth());
            }
        }
        final SSLParameters params = socket.getSSLParameters();
        configureParams(params);
        socket.setSSLParameters(params);
    }

    /**
     * Here one can do some unencrypted actions when STARTTLS is enabled to then start the handshake manually.
     */
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
        final ISignatureAlgorithm signatureAlgorithm = getSignatureAlgorithm();
        final ClientAuth clientAuth = getClientAuth();
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
            final SSLContext context = newContextFromProvider();
            final KeyManager[] keyManagers;
            final TrustManager[] trustManagers;
            if (server) {
                final KeyManagerFactory kmf = KeyStores.buildKeyManagerFactory(keyPair.getPrivate(), serverCertificate);
                keyManagers = kmf.getKeyManagers();
                if (mTls) {
                    final TrustManagerFactory tmf = KeyStores.buildTrustManagerFactory(serverCertificate);
                    trustManagers = tmf.getTrustManagers();
                } else {
                    trustManagers = null;
                }
            } else {
                final TrustManagerFactory tmf = KeyStores.buildTrustManagerFactory(serverCertificate);
                trustManagers = tmf.getTrustManagers();
                if (mTls) {
                    final KeyManagerFactory kmf = KeyStores.buildKeyManagerFactory(keyPair.getPrivate(),
                            serverCertificate);
                    keyManagers = kmf.getKeyManagers();
                } else {
                    keyManagers = null;
                }
            }
            context.init(keyManagers, trustManagers, CryptoRandomGenerators.newCryptoRandom());
            return context;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ITlsProtocol getProtocol() {
        return TlsProtocol.DEFAULT;
    }

    @Override
    public HandshakeValidation getHandshakeValidation() {
        return HandshakeValidation.DEFAULT.withDerivedPassword(getHostname());
    }

    /**
     * Override this to use a different provider.
     */
    protected SSLContext newContextFromProvider() throws NoSuchAlgorithmException {
        return SSLContext.getInstance(getProtocol().getFamily());
    }

    @Override
    public SSLEngine newEngine() {
        final SSLContext context = newContext();
        final SSLEngine engine;
        if (server) {
            engine = context.createSSLEngine();
        } else {
            engine = context.createSSLEngine(socketAddress.getHostName(), socketAddress.getPort());
        }
        engine.setUseClientMode(!isServer());
        if (isServer()) {
            switch (getClientAuth()) {
            case NEED:
                engine.setNeedClientAuth(true);
                break;
            case WANT:
                engine.setWantClientAuth(true);
                break;
            case NONE:
                break;
            default:
                throw UnknownArgumentException.newInstance(ClientAuth.class, getClientAuth());
            }
        }
        final SSLParameters params = engine.getSSLParameters();
        configureParams(params);
        engine.setSSLParameters(params);
        return engine;
    }

    protected void configureParams(final SSLParameters params) {
        if (getProtocol().isVersioned()) {
            params.setProtocols(new String[] { getProtocol().getName() });
        }
        final Integer maximumPacketSize = getMaximumPacketSize();
        if (maximumPacketSize != null) {
            params.setMaximumPacketSize(maximumPacketSize);
        }
    }

    /**
     * Can be configured when actually using datagrams as transport or something different restricts the packet size
     * (e.g. mapped memory).
     */
    protected Integer getMaximumPacketSize() {
        //        return BlockingDatagramSynchronousChannel.MAX_UNFRAGMENTED_PACKET_SIZE;
        return null;
    }

    @Override
    public boolean isStartTlsEnabled() {
        return CryptoProperties.DEFAULT_START_TLS_ENABLED;
    }

    protected byte[] getDerivedKeyPepper() {
        return CryptoProperties.DEFAULT_PEPPER;
    }

    protected byte[] getDerivedKeyPassword() {
        return DEFAULT_DERIVED_KEY_PASSWORD.getBytes();
    }

    protected byte[] getDerivedKeyInfo() {
        return DEFAULT_DERIVED_KEY_INFO.getBytes();
    }

    protected ISignatureAlgorithm getSignatureAlgorithm() {
        return DEFAULT_SIGNATURE_ALGORITHM;
    }

    protected ClientAuth getClientAuth() {
        return DEFAULT_CLIENT_AUTH;
    }

    protected String getHostname() {
        if (server) {
            return IntegrationProperties.HOSTNAME;
        } else {
            return socketAddress.getHostName();
        }
    }

}
