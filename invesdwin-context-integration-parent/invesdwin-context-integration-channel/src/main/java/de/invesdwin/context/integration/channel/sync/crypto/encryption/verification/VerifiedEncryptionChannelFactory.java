package de.invesdwin.context.integration.channel.sync.crypto.encryption.verification;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.encryption.cipher.symmetric.SymmetricEncryptionFactory;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.context.security.crypto.verification.hash.HashVerificationFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class VerifiedEncryptionChannelFactory
        implements ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> {

    private final IEncryptionFactory encryptionFactory;
    private final IVerificationFactory verificationFactory;

    public VerifiedEncryptionChannelFactory(final IEncryptionFactory encryptionFactory,
            final IVerificationFactory verificationFactory) {
        this.encryptionFactory = encryptionFactory;
        this.verificationFactory = verificationFactory;
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newReader(final ISynchronousReader<IByteBufferProvider> reader) {
        return new VerifiedEncryptionSynchronousReader(reader, encryptionFactory, verificationFactory);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newWriter(final ISynchronousWriter<IByteBufferProvider> writer) {
        return new VerifiedEncryptionSynchronousWriter(writer, encryptionFactory, verificationFactory);
    }

    public static VerifiedEncryptionChannelFactory fromPassword(final String password) {
        final DerivedKeyProvider derivedKeyProvider = DerivedKeyProvider.fromPassword(CryptoProperties.DEFAULT_PEPPER,
                password);
        return new VerifiedEncryptionChannelFactory(new SymmetricEncryptionFactory(derivedKeyProvider),
                new HashVerificationFactory(derivedKeyProvider));
    }

}
