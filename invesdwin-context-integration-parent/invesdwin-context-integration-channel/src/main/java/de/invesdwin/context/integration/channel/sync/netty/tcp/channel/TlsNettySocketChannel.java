package de.invesdwin.context.integration.channel.sync.netty.tcp.channel;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.annotation.concurrent.Immutable;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.operator.OperatorCreationException;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.IDerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.certificate.SelfSignedCertGenerator;
import de.invesdwin.context.security.crypto.verification.signature.SignatureKey;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.EcdsaAlgorithm;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.ISignatureAlgorithm;
import de.invesdwin.util.time.range.TimeRange;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;

@Immutable
public class TlsNettySocketChannel extends NettySocketChannel {

    public TlsNettySocketChannel(final INettySocketChannelType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
        //unsafe write not supported, this would circumvent the ssl handler
        setKeepBootstrapRunningAfterOpen();
    }

    @Override
    protected void onSocketChannel(final SocketChannel socketChannel) {
        final ChannelPipeline pipeline = socketChannel.pipeline();

        final SSLEngine engine = newSSLEngine(socketChannel);
        engine.setUseClientMode(!server);

        pipeline.addLast(new SslHandler(engine));

        super.onSocketChannel(socketChannel);
    }

    /**
     * WARNING: With this implementation the confidentiality relies solely on the CryptoProperties.DEFAULT_PEPPER. It
     * would be advisable to use a pre shared certificate so that the client does not know how to generate the private
     * key. Especially when the process runs on untrusted client hardware. So override this implementation to do
     * something more secure.
     */
    protected SSLEngine newSSLEngine(final SocketChannel socketChannel) {
        //netty does not support EdDSA: https://github.com/netty/netty/issues/10916
        final SslProvider sslProvider = getSslProvider();
        final ISignatureAlgorithm signatureAlgorithm = EcdsaAlgorithm.DEFAULT;
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
        final IDerivedKeyProvider derivedKeyProvider = DerivedKeyProvider.fromPassword(CryptoProperties.DEFAULT_PEPPER,
                ("netty-ssl-engine-password").getBytes());
        final SignatureKey signatureKey = derivedKeyProvider.newDerivedKey(signatureAlgorithm,
                "netty-ssl-engine-key".getBytes(), signatureAlgorithm.getDefaultKeySizeBits());
        final KeyPair keyPair = new KeyPair(signatureKey.getVerifyKey(), signatureKey.getSignKey());

        try {
            final TimeRange validity = SelfSignedCertGenerator.newBrowserValidity();
            final String hostname = getHostname();
            final X509Certificate serverCertificate = SelfSignedCertGenerator.generate(keyPair,
                    signatureAlgorithm.getAlgorithm(), hostname, validity);
            if (server) {
                final SslContext sslContext = SslContextBuilder.forServer(keyPair.getPrivate(), serverCertificate)
                        .sslProvider(sslProvider)
                        .build();
                return sslContext.newEngine(socketChannel.alloc());
            } else {
                final SslContext sslContext = SslContextBuilder.forClient()
                        .trustManager(serverCertificate)
                        .sslProvider(sslProvider)
                        .build();
                return sslContext.newEngine(socketChannel.alloc(), socketAddress.getHostName(),
                        socketAddress.getPort());
            }
        } catch (OperatorCreationException | CertificateException | CertIOException | SSLException e) {
            throw new RuntimeException(e);
        }
    }

    protected String getHostname() {
        if (server) {
            return IntegrationProperties.HOSTNAME;
        } else {
            return socketAddress.getHostName();
        }
    }

    protected SslProvider getSslProvider() {
        if (server) {
            return SslContext.defaultServerProvider();
        } else {
            return SslContext.defaultClientProvider();
        }
    }

}
