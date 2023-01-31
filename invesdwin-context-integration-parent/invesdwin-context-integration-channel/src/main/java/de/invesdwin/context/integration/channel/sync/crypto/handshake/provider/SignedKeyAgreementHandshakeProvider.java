package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider;

import java.io.IOException;
import java.security.PublicKey;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousReader;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.VerifiedEncryptionChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.security.crypto.encryption.cipher.symmetric.SymmetricEncryptionFactory;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.IDerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.derivation.IDerivationFactory;
import de.invesdwin.context.security.crypto.random.CryptoRandomGenerator;
import de.invesdwin.context.security.crypto.random.CryptoRandomGenerators;
import de.invesdwin.context.security.crypto.verification.signature.SignatureKey;
import de.invesdwin.context.security.crypto.verification.signature.SignatureVerificationFactory;
import de.invesdwin.context.security.crypto.verification.signature.algorithm.ISignatureAlgorithm;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * This implementation achieves forward security and non-repudiation even if the pre shared pepper and password are
 * compromised afterwards. This will use additional public/private keys with signatures to make sure the authentication
 * is safe against man-in-the-middle-attacks. This is done with non-repudiation during the key exchange using ephemeral
 * keys.
 * 
 * Static/PreShared public/private keys can also be used, but this will be insecure once a private key is compromised.
 * 
 * https://security.stackexchange.com/questions/14731/what-is-ecdhe-rsa (though we use something like Ed25519 instead of
 * RSA per default)
 * 
 * https://crypto.stackexchange.com/questions/90384/method-to-mitigate-mitm-attack-for-dh-key-exchange (should also work
 * with static/preShared keys or certificates if such are provided here)
 */
@Immutable
public class SignedKeyAgreementHandshakeProvider extends AKeyAgreementHandshakeProvider {

    private final AKeyAgreementHandshakeProvider unsignedProvider;

    /**
     * WARNING: should use DerivedSignedKeyAgreementHandshake instead as a simpler approach that achieves the same. This
     * is rather an example that can be customized via extension.
     */
    @Deprecated
    public SignedKeyAgreementHandshakeProvider(final AKeyAgreementHandshakeProvider unsignedProvider) {
        super(unsignedProvider.getHandshakeTimeout(), unsignedProvider.getSessionIdentifier());
        this.unsignedProvider = unsignedProvider;
    }

    @Override
    protected void performHandshake(final HandshakeChannel channel,
            final IgnoreOpenCloseSynchronousWriter<IByteBufferProvider> underlyingWriter,
            final ISynchronousWriter<IByteBufferProvider> unsignedHandshakeWriter,
            final IgnoreOpenCloseSynchronousReader<IByteBufferProvider> underlyingReader,
            final ISynchronousReader<IByteBufferProvider> unsignedHandshakeReader) throws IOException {
        final SignatureKey ourSignatureKey = getOurSignatureKey();
        final PublicKey staticOtherVerifyKey = getOtherVerifyKey(ourSignatureKey);
        final ISynchronousWriter<IByteBufferProvider> signedHandshakeWriter;
        final ISynchronousReader<IByteBufferProvider> signedHandshakeReader;
        if (staticOtherVerifyKey != null) {
            final SignatureKey handshakeWriterSignatureKey = new SignatureKey(ourSignatureKey.getAlgorithm(), null,
                    ourSignatureKey.getSignKey(), ourSignatureKey.getKeySizeBits());
            signedHandshakeWriter = newSignedHandshakeChannelFactory(handshakeWriterSignatureKey)
                    .newWriter(underlyingWriter);
            final SignatureKey handshakeReaderSignatureKey = new SignatureKey(ourSignatureKey.getAlgorithm(),
                    staticOtherVerifyKey, null, ourSignatureKey.getKeySizeBits());
            signedHandshakeReader = newSignedHandshakeChannelFactory(handshakeReaderSignatureKey)
                    .newReader(underlyingReader);
        } else {
            unsignedHandshakeWriter.open();
            try {
                unsignedHandshakeReader.open();
                try {

                    final byte[] ourVerifyKey = ourSignatureKey.getVerifyKey().getEncoded();
                    final IByteBuffer ourVerifyKeyMessage = ByteBuffers.wrap(ourVerifyKey);
                    unsignedHandshakeWriter.write(ourVerifyKeyMessage);
                    final ASpinWait writerSpinWait = newSpinWait(unsignedHandshakeWriter);
                    try {
                        if (!writerSpinWait.awaitFulfill(System.nanoTime(), getHandshakeTimeout())) {
                            throw new TimeoutException(
                                    "Write handshake message timeout exceeded: " + getHandshakeTimeout());
                        }
                    } catch (final IOException e) {
                        throw e;
                    } catch (final Exception e) {
                        throw new IOException(e);
                    }

                    final ASpinWait readerSpinWait = newSpinWait(unsignedHandshakeReader);
                    try {
                        if (!readerSpinWait.awaitFulfill(System.nanoTime(), getHandshakeTimeout())) {
                            throw new TimeoutException(
                                    "Read handshake message timeout exceeded: " + getHandshakeTimeout());
                        }
                    } catch (final IOException e) {
                        throw e;
                    } catch (final Exception e) {
                        throw new IOException(e);
                    }
                    final IByteBuffer otherVerifyKeyMessage = unsignedHandshakeReader.readMessage().asBuffer();
                    final PublicKey otherVerifyKey = SignatureKey.wrapVerifyKey(
                            ourSignatureKey.getAlgorithm().getKeyAlgorithm(), otherVerifyKeyMessage.asByteArray());
                    unsignedHandshakeReader.readFinished();

                    final SignatureKey handshakeWriterSignatureKey = new SignatureKey(ourSignatureKey.getAlgorithm(),
                            null, ourSignatureKey.getSignKey(), ourSignatureKey.getKeySizeBits());
                    signedHandshakeWriter = newSignedHandshakeChannelFactory(handshakeWriterSignatureKey)
                            .newWriter(underlyingWriter);
                    final SignatureKey handshakeReaderSignatureKey = new SignatureKey(ourSignatureKey.getAlgorithm(),
                            otherVerifyKey, null, ourSignatureKey.getKeySizeBits());
                    signedHandshakeReader = newSignedHandshakeChannelFactory(handshakeReaderSignatureKey)
                            .newReader(underlyingReader);
                } finally {
                    Closeables.closeQuietly(unsignedHandshakeReader);
                }
            } finally {
                Closeables.closeQuietly(unsignedHandshakeWriter);
            }
        }
        super.performHandshake(channel, underlyingWriter, signedHandshakeWriter, underlyingReader,
                signedHandshakeReader);
    }

    /**
     * One could override this method to return a static key here instead. Though using ephemeral keys here will lead to
     * better security.
     * 
     * Using the pepper here as a password makes this approach similar to J-PAKE in its approach of a two step password
     * authenticated key exchange. Though we use a signature verification instead of a key confirmation to verify the
     * common shared secret (pepper). (https://en.wikipedia.org/wiki/Password_Authenticated_Key_Exchange_by_Juggling)
     */
    protected SignatureKey getOurSignatureKey() {
        final ISignatureAlgorithm signatureAlgorithm = getSignatureAlgorithm();
        final byte[] ourRandomKey = ByteBuffers.allocateByteArray(IDerivationFactory.getDefault().getExtractLength());
        final CryptoRandomGenerator random = CryptoRandomGenerators.getThreadLocalCryptoRandom();
        random.nextBytes(ourRandomKey);

        final DerivedKeyProvider ourDerivedKeyProvider = DerivedKeyProvider.fromRandom(getPepper(), ourRandomKey);
        final SignatureKey ourSignatureKey = new SignatureKey(signatureAlgorithm, ourDerivedKeyProvider);
        return ourSignatureKey;
    }

    /**
     * Return a static/preShared key here to authenticate the other side without needing to send over the public key. In
     * that case
     */
    protected PublicKey getOtherVerifyKey(final SignatureKey ourSignatureKey) {
        return null;
    }

    /**
     * We use e.g. ED25519 (EdDSA) here per default.
     */
    protected ISignatureAlgorithm getSignatureAlgorithm() {
        return ISignatureAlgorithm.getDefault();
    }

    protected ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> newSignedHandshakeChannelFactory(
            final SignatureKey signatureKey) {
        final DerivedKeyProvider symmetricDerivedKeyProvider = DerivedKeyProvider.fromPassword(getPepper(),
                "signed-handshake-" + getSessionIdentifier());
        return new VerifiedEncryptionChannelFactory(new SymmetricEncryptionFactory(symmetricDerivedKeyProvider),
                new SignatureVerificationFactory(signatureKey));
    }

    /**
     * One could override this with DisabledChannelFactory to not require a pre shared pepper/password. This would allow
     * anyone to open connections and it would allow Man-in-the-Middle attacks where one replaces both sides public
     * signature keys with his own. DisabledChannelFactory can only be used when getOtherSignatureKey returns a pre
     * shared public key for the other side.
     * 
     * SSH achieves security against Man-in-the-Middle-Attacks by checking the fingerprint on subsequent connections.
     * Though someone could still impersonate the first connection attempt.
     */
    @Override
    public ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> newAuthenticatedHandshakeChannelFactory() {
        return unsignedProvider.newAuthenticatedHandshakeChannelFactory();
    }

    @Override
    public ASpinWait newSpinWait(final ISynchronousReader<IByteBufferProvider> delegate) {
        return unsignedProvider.newSpinWait(delegate);
    }

    @Override
    public ASpinWait newSpinWait(final ISynchronousWriter<IByteBufferProvider> delegate) {
        return unsignedProvider.newSpinWait(delegate);
    }

    @Override
    public int getKeySizeBits() {
        return unsignedProvider.getKeySizeBits();
    }

    @Override
    public String getKeyAgreementAlgorithm() {
        return unsignedProvider.getKeyAgreementAlgorithm();
    }

    @Override
    public String getKeyAlgorithm() {
        return unsignedProvider.getKeyAlgorithm();
    }

    @Override
    public ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> newEncryptedChannelFactory(
            final IDerivedKeyProvider derivedKeyProvider) {
        return unsignedProvider.newEncryptedChannelFactory(derivedKeyProvider);
    }

}
