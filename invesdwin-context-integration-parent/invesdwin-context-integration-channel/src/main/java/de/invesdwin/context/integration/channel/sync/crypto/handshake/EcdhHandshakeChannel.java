package de.invesdwin.context.integration.channel.sync.crypto.handshake;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.crypto.KeyAgreement;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousReader;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.stream.StreamVerifiedEncryptionChannelFactory;
import de.invesdwin.context.security.crypto.encryption.cipher.asymmetric.AsymmetricCipherKey;
import de.invesdwin.context.security.crypto.encryption.cipher.symmetric.SymmetricEncryptionFactory;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.IDerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.derivation.IDerivationFactory;
import de.invesdwin.context.security.crypto.verification.hash.HashVerificationFactory;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.EcdsaAlgorithm;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.EcdsaKeySize;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class EcdhHandshakeChannel implements ISynchronousReader<IByteBuffer>, ISynchronousWriter<IByteBufferWriter> {

    private static final String DEFAULT_KEY_ALGORITHM = EcdsaAlgorithm.DEFAULT.getKeyAlgorithm();
    private static final String DEFAULT_KEY_AGREEMENT_ALGORITHM = "ECDH";

    private final ISynchronousReader<IByteBuffer> underlyingReader;
    private final ISynchronousWriter<IByteBufferWriter> underlyingWriter;
    private final Duration handshakeTimeout;
    private final EcdsaKeySize keySize;

    private ISynchronousReader<IByteBuffer> encryptedReader;
    private ISynchronousWriter<IByteBufferWriter> encryptedWriter;

    public EcdhHandshakeChannel(final ISynchronousReader<IByteBuffer> underlyingReader,
            final ISynchronousWriter<IByteBufferWriter> underlyingWriter, final EcdsaKeySize keySize,
            final Duration handshakeTimeout) {
        this.underlyingReader = underlyingReader;
        this.underlyingWriter = underlyingWriter;
        this.keySize = keySize;
        this.handshakeTimeout = handshakeTimeout;
    }

    @Override
    public void open() throws IOException {
        if (encryptedWriter == null) {
            underlyingReader.open();
            underlyingWriter.open();
            handshake();
        }
    }

    private void handshake() throws IOException {
        final ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> handshakeChannelFactory = newHandshakeChannelFactory();
        final IgnoreOpenCloseSynchronousWriter<IByteBufferWriter> ignoreOpenCloseWriter = IgnoreOpenCloseSynchronousWriter
                .valueOf(underlyingWriter);
        final ISynchronousWriter<IByteBufferWriter> handshakeWriter = handshakeChannelFactory
                .newWriter(ignoreOpenCloseWriter);
        handshakeWriter.open();
        try {
            final IgnoreOpenCloseSynchronousReader<IByteBuffer> ignoreOpenCloseReader = IgnoreOpenCloseSynchronousReader
                    .valueOf(underlyingReader);
            final ISynchronousReader<IByteBuffer> handshakeReader = handshakeChannelFactory
                    .newReader(ignoreOpenCloseReader);
            handshakeReader.open();
            try {
                final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(getKeyAlgorithm());
                keyPairGenerator.initialize(keySize.getBits());
                final KeyPair ephemeralKeyPair = keyPairGenerator.generateKeyPair();

                final byte[] ourPublicKey = ephemeralKeyPair.getPublic().getEncoded();
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
                otherPublicKeyMessage.clear();

                // Perform key agreement
                final KeyAgreement ka = KeyAgreement.getInstance(getKeyAgreementAlgorithm());
                ka.init(ephemeralKeyPair.getPrivate());
                ka.doPhase(otherPublicKey, true);

                // Read shared secret
                final byte[] sharedSecret = ka.generateSecret();

                final DerivedKeyProvider derivedKeyProvider = new DerivedKeyProvider(sharedSecret,
                        getDerivationFactory());

                final ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> encryptedChannelFactory = newEncryptedChannelFactory(
                        derivedKeyProvider);
                this.encryptedWriter = encryptedChannelFactory.newWriter(ignoreOpenCloseWriter);
                this.encryptedWriter.open();
                this.encryptedReader = encryptedChannelFactory.newReader(ignoreOpenCloseReader);
                this.encryptedReader.open();
            } catch (final NoSuchAlgorithmException | IOException | InvalidKeyException e) {
                throw new RuntimeException(e);
            } finally {
                Closeables.closeQuietly(handshakeReader);
            }
        } finally {
            Closeables.closeQuietly(handshakeWriter);
        }
    }

    protected ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> newHandshakeChannelFactory() {
        //this prevents unauthorized clients from connecting that do not know the pre shared pepper and password
        return new EncryptedHandshakeChannelFactory("handshake");
    }

    protected ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> newEncryptedChannelFactory(
            final IDerivedKeyProvider derivedKeyProvider) {
        return new StreamVerifiedEncryptionChannelFactory(new SymmetricEncryptionFactory(derivedKeyProvider),
                new HashVerificationFactory(derivedKeyProvider));
    }

    /**
     * Override this to disable spinning or configure type of waits.
     */
    protected ASpinWait newSpinWait(final ISynchronousReader<IByteBuffer> delegate) {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return delegate.hasNext();
            }
        };
    }

    protected String getKeyAgreementAlgorithm() {
        return DEFAULT_KEY_AGREEMENT_ALGORITHM;
    }

    protected String getKeyAlgorithm() {
        return DEFAULT_KEY_ALGORITHM;
    }

    protected IDerivationFactory getDerivationFactory() {
        return IDerivationFactory.DEFAULT;
    }

    @Override
    public void close() throws IOException {
        if (encryptedWriter != null) {
            encryptedWriter.close();
            encryptedWriter = null;
            encryptedReader.close();
            encryptedReader = null;
            underlyingWriter.close();
            underlyingReader.close();
        }
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        encryptedWriter.write(message);
    }

    @Override
    public boolean hasNext() throws IOException {
        return encryptedReader.hasNext();
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        return encryptedReader.readMessage();
    }

    @Override
    public void readFinished() {
        encryptedReader.readFinished();
    }

}
