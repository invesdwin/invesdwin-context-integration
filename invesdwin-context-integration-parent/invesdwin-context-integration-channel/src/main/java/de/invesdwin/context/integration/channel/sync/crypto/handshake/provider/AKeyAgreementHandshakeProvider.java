package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;
import javax.crypto.KeyAgreement;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousReader;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.VerifiedEncryptionChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.security.crypto.encryption.cipher.asymmetric.AsymmetricCipherKey;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public abstract class AKeyAgreementHandshakeProvider extends AKeyExchangeHandshakeProvider {

    public AKeyAgreementHandshakeProvider(final Duration handshakeTimeout, final String sessionIdentifier) {
        super(handshakeTimeout, sessionIdentifier);
    }

    public abstract int getKeySizeBits();

    public abstract String getKeyAgreementAlgorithm();

    public abstract String getKeyAlgorithm();

    /**
     * https://neilmadden.blog/2016/05/20/ephemeral-elliptic-curve-diffie-hellman-key-agreement-in-java/
     */
    @Override
    protected void performHandshake(final HandshakeChannel channel,
            final IgnoreOpenCloseSynchronousWriter<IByteBufferProvider> ignoreOpenCloseWriter,
            final ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            final IgnoreOpenCloseSynchronousReader<IByteBuffer> ignoreOpenCloseReader,
            final ISynchronousReader<IByteBuffer> handshakeReader) throws IOException {
        handshakeWriter.open();
        try {
            handshakeReader.open();
            try {
                final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(getKeyAlgorithm());
                keyPairGenerator.initialize(getKeySizeBits());
                final KeyPair ourKeyPair = keyPairGenerator.generateKeyPair();

                final byte[] ourPublicKey = ourKeyPair.getPublic().getEncoded();
                final IByteBuffer ourPublicKeyMessage = ByteBuffers.wrap(ourPublicKey);
                handshakeWriter.write(ourPublicKeyMessage);

                final ASpinWait spinWait = newSpinWait(handshakeReader);
                try {
                    if (!spinWait.awaitFulfill(System.nanoTime(), getHandshakeTimeout())) {
                        throw new TimeoutException("Read handshake message timeout exceeded: " + getHandshakeTimeout());
                    }
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
                final IByteBuffer otherPublicKeyMessage = handshakeReader.readMessage();
                final PublicKey otherPublicKey = AsymmetricCipherKey.wrapPublicKey(getKeyAlgorithm(),
                        otherPublicKeyMessage.asByteArray());
                handshakeReader.readFinished();

                // Perform key agreement
                final KeyAgreement ka = KeyAgreement.getInstance(getKeyAgreementAlgorithm());
                ka.init(ourKeyPair.getPrivate());
                ka.doPhase(otherPublicKey, true);

                // Read shared secret
                final byte[] sharedSecret = ka.generateSecret();

                final DerivedKeyProvider derivedKeyProvider = new DerivedKeyProvider(sharedSecret,
                        getDerivationFactory());
                finishHandshake(channel, ignoreOpenCloseWriter, ignoreOpenCloseReader, derivedKeyProvider);
            } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
                throw new IOException(e);
            } finally {
                Closeables.closeQuietly(handshakeReader);
            }
        } finally {
            Closeables.closeQuietly(handshakeWriter);
        }
    }

    /**
     * Encryption here prevents unauthorized clients from connecting that do not know the pre shared pepper and
     * password. We use the static password to authenticate the handshake, then use ephemeral-ephemeral ecdh to create a
     * session key for forward security. See "7.7. Payload security properties" in
     * http://www.noiseprotocol.org/noise.html for more alternatives.
     * 
     * To achieve forward security and non-repudiation even if the pre shared pepper and password are compromised, use
     * SignedKeyAgreementHandshake instead.
     */
    @Override
    public ISynchronousChannelFactory<IByteBuffer, IByteBufferProvider> newAuthenticatedHandshakeChannelFactory() {
        return VerifiedEncryptionChannelFactory.fromPassword("authenticated-handshake-" + getSessionIdentifier());
    }

}
