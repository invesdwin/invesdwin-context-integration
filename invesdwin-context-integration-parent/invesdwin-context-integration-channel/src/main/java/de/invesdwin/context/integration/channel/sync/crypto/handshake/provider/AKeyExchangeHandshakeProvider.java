package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousReader;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.VerifiedEncryptionChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.stream.StreamVerifiedEncryptionChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.context.security.crypto.encryption.cipher.symmetric.SymmetricEncryptionFactory;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.IDerivedKeyProvider;
import de.invesdwin.context.security.crypto.key.derivation.IDerivationFactory;
import de.invesdwin.context.security.crypto.verification.hash.HashVerificationFactory;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public abstract class AKeyExchangeHandshakeProvider implements IHandshakeProvider {

    private final Duration handshakeTimeout;
    private final String sessionIdentifier;

    public AKeyExchangeHandshakeProvider(final Duration handshakeTimeout, final String sessionIdentifier) {
        this.handshakeTimeout = handshakeTimeout;
        this.sessionIdentifier = sessionIdentifier;
    }

    @Override
    public Duration getHandshakeTimeout() {
        return handshakeTimeout;
    }

    public byte[] getPepper() {
        return CryptoProperties.DEFAULT_PEPPER;
    }

    public String getSessionIdentifier() {
        return sessionIdentifier;
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

    public IDerivationFactory getDerivationFactory() {
        return IDerivationFactory.getDefault();
    }

    @Override
    public void handshake(final HandshakeChannel channel) throws IOException {
        final IgnoreOpenCloseSynchronousWriter<IByteBufferProvider> underlyingWriter = IgnoreOpenCloseSynchronousWriter
                .valueOf(channel.getWriter().getUnderlyingWriter());
        final IgnoreOpenCloseSynchronousReader<IByteBuffer> underlyingReader = IgnoreOpenCloseSynchronousReader
                .valueOf(channel.getReader().getUnderlyingReader());

        final ISynchronousChannelFactory<IByteBuffer, IByteBufferProvider> handshakeChannelFactory = newAuthenticatedHandshakeChannelFactory();
        final ISynchronousWriter<IByteBufferProvider> handshakeWriter = handshakeChannelFactory
                .newWriter(underlyingWriter);
        final ISynchronousReader<IByteBuffer> handshakeReader = handshakeChannelFactory.newReader(underlyingReader);
        performHandshake(channel, underlyingWriter, handshakeWriter, underlyingReader, handshakeReader);
    }

    protected abstract void performHandshake(HandshakeChannel channel,
            IgnoreOpenCloseSynchronousWriter<IByteBufferProvider> underlyingWriter,
            ISynchronousWriter<IByteBufferProvider> handshakeWriter,
            IgnoreOpenCloseSynchronousReader<IByteBuffer> underlyingReader,
            ISynchronousReader<IByteBuffer> handshakeReader) throws IOException;

    protected void finishHandshake(final HandshakeChannel channel,
            final IgnoreOpenCloseSynchronousWriter<IByteBufferProvider> underlyingWriter,
            final IgnoreOpenCloseSynchronousReader<IByteBuffer> underlyingReader,
            final DerivedKeyProvider derivedKeyProvider) throws IOException {
        final ISynchronousChannelFactory<IByteBuffer, IByteBufferProvider> encryptedChannelFactory = newEncryptedChannelFactory(
                derivedKeyProvider);
        final ISynchronousReader<IByteBuffer> encryptedReader = encryptedChannelFactory.newReader(underlyingReader);
        final ISynchronousWriter<IByteBufferProvider> encryptedWriter = encryptedChannelFactory
                .newWriter(underlyingWriter);
        channel.getReader().setEncryptedReader(encryptedReader);
        channel.getWriter().setEncryptedWriter(encryptedWriter);
        encryptedReader.open();
        encryptedWriter.open();
    }

    /**
     * Encryption here prevents unauthorized clients from connecting that do not know the pre shared pepper and
     * password. We use the static password to authenticate the handshake, then use ephemeral-ephemeral ecdh to create a
     * session key for forward security. See "7.7. Payload security properties" in
     * http://www.noiseprotocol.org/noise.html for more alternatives.
     * 
     * To achieve forward security and non-repudiation even if the pre shared pepper and password are compromised, use
     * SignedKeyAgreementHandshake instead.
     * 
     * Can use DisabledSynchronousChannelFactory here if the algorithm authenticates itself (which is the default here).
     */
    /**
     * Encryption here prevents unauthorized clients from connecting that do not know the pre shared pepper and
     * password. We use the static password to authenticate the handshake, then use ephemeral-ephemeral ecdh to create a
     * session key for forward security. See "7.7. Payload security properties" in
     * http://www.noiseprotocol.org/noise.html for more alternatives.
     * 
     * To achieve forward security and non-repudiation even if the pre shared pepper and password are compromised, use
     * SignedKeyAgreementHandshake instead.
     */
    public ISynchronousChannelFactory<IByteBuffer, IByteBufferProvider> newAuthenticatedHandshakeChannelFactory() {
        return VerifiedEncryptionChannelFactory.fromPassword("authenticated-handshake-" + getSessionIdentifier());
    }

    /**
     * We use something like AES128+HMAC_SHA256 here so that messages can not be tampered with. One could also use
     * ED25519 signatures instead of HMAC_SHA256 hashes for non-repudiation.
     */
    public ISynchronousChannelFactory<IByteBuffer, IByteBufferProvider> newEncryptedChannelFactory(
            final IDerivedKeyProvider derivedKeyProvider) {
        return new StreamVerifiedEncryptionChannelFactory(new SymmetricEncryptionFactory(derivedKeyProvider),
                new HashVerificationFactory(derivedKeyProvider));
    }

}
