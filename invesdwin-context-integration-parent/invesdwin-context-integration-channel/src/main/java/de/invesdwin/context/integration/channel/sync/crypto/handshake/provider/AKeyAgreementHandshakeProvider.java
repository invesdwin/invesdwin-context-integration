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
import de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.stream.StreamVerifiedEncryptionChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.security.crypto.encryption.cipher.asymmetric.AsymmetricCipherKey;
import de.invesdwin.context.security.crypto.encryption.cipher.symmetric.SymmetricEncryptionFactory;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.IDerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.derivation.IDerivationFactory;
import de.invesdwin.context.security.crypto.verification.hash.HashVerificationFactory;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public abstract class AKeyAgreementHandshakeProvider implements IHandshakeProvider {

    private final Duration handshakeTimeout;

    public AKeyAgreementHandshakeProvider(final Duration handshakeTimeout) {
        this.handshakeTimeout = handshakeTimeout;
    }

    @Override
    public Duration getHandshakeTimeout() {
        return handshakeTimeout;
    }

    /**
     * Override this to disable spinning or configure type of waits.
     */
    public ASpinWait newSpinWait(final ISynchronousReader<IByteBuffer> delegate) {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return delegate.hasNext();
            }
        };
    }

    public abstract int getKeySizeBits();

    public abstract String getKeyAgreementAlgorithm();

    public abstract String getKeyAlgorithm();

    public IDerivationFactory getDerivationFactory() {
        return IDerivationFactory.getDefault();
    }

    @Override
    public void handshake(final HandshakeChannel channel) throws IOException {
        synchronized (channel) {
            final IgnoreOpenCloseSynchronousWriter<IByteBufferWriter> ignoreOpenCloseWriter = IgnoreOpenCloseSynchronousWriter
                    .valueOf(channel.getWriter().getUnderlyingWriter());
            final IgnoreOpenCloseSynchronousReader<IByteBuffer> ignoreOpenCloseReader = IgnoreOpenCloseSynchronousReader
                    .valueOf(channel.getReader().getUnderlyingReader());

            final ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> handshakeChannelFactory = newAuthenticatedHandshakeChannelFactory();
            final ISynchronousWriter<IByteBufferWriter> handshakeWriter = handshakeChannelFactory
                    .newWriter(ignoreOpenCloseWriter);
            final ISynchronousReader<IByteBuffer> handshakeReader = handshakeChannelFactory
                    .newReader(ignoreOpenCloseReader);
            performHandshake(channel, ignoreOpenCloseWriter, handshakeWriter, ignoreOpenCloseReader, handshakeReader);
        }
    }

    /**
     * https://neilmadden.blog/2016/05/20/ephemeral-elliptic-curve-diffie-hellman-key-agreement-in-java/
     */
    protected void performHandshake(final HandshakeChannel channel,
            final IgnoreOpenCloseSynchronousWriter<IByteBufferWriter> ignoreOpenCloseWriter,
            final ISynchronousWriter<IByteBufferWriter> handshakeWriter,
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
                    if (!spinWait.awaitFulfill(System.nanoTime(), handshakeTimeout)) {
                        throw new TimeoutException("Read handshake message timeout exceeded: " + handshakeTimeout);
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

                final ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> encryptedChannelFactory = newEncryptedChannelFactory(
                        derivedKeyProvider);
                channel.getWriter().setEncryptedWriter(encryptedChannelFactory.newWriter(ignoreOpenCloseWriter));
                channel.getWriter().getEncryptedWriter().open();
                channel.getReader().setEncryptedReader(encryptedChannelFactory.newReader(ignoreOpenCloseReader));
                channel.getReader().getEncryptedReader().open();
            } catch (final NoSuchAlgorithmException | IOException | InvalidKeyException e) {
                throw new RuntimeException(e);
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
    public ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> newAuthenticatedHandshakeChannelFactory() {
        return VerifiedEncryptionChannelFactory.fromPassword("handshake-" + getKeyAgreementAlgorithm());
    }

    /**
     * We use something like AES128+HMAC_SHA256 here so that messages can not be tampered with. One could also use
     * ED25519 signatures instead of HMAC_SHA256 hashes for non-repudiation.
     */
    public ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> newEncryptedChannelFactory(
            final IDerivedKeyProvider derivedKeyProvider) {
        return new StreamVerifiedEncryptionChannelFactory(new SymmetricEncryptionFactory(derivedKeyProvider),
                new HashVerificationFactory(derivedKeyProvider));
    }

    public SignedKeyAgreementHandshakeProvider asSigned() {
        return SignedKeyAgreementHandshakeProvider.valueOf(this);
    }

}
