package de.invesdwin.context.integration.channel.sync.crypto.encryption;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.CryptoProperties;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.encryption.cipher.symmetric.SymmetricEncryptionFactory;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@Immutable
public class EncryptionChannelFactory implements ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> {

    private final IEncryptionFactory encryptionFactory;

    public EncryptionChannelFactory(final IEncryptionFactory encryptionFactory) {
        this.encryptionFactory = encryptionFactory;
    }

    @Override
    public ISynchronousReader<IByteBuffer> newReader(final ISynchronousReader<IByteBuffer> reader) {
        return new EncryptionSynchronousReader(reader, encryptionFactory);
    }

    @Override
    public ISynchronousWriter<IByteBufferWriter> newWriter(final ISynchronousWriter<IByteBufferWriter> writer) {
        return new EncryptionSynchronousWriter(writer, encryptionFactory);
    }

    public static EncryptionChannelFactory fromPassword(final String password) {
        final DerivedKeyProvider derivedKeyProvider = DerivedKeyProvider.fromPassword(CryptoProperties.DEFAULT_PEPPER,
                password);
        return new EncryptionChannelFactory(new SymmetricEncryptionFactory(derivedKeyProvider));
    }

}