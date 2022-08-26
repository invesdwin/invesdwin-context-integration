package de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.stream;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class StreamVerifiedEncryptionChannelFactory
        implements ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> {

    private final IEncryptionFactory encryptionFactory;
    private final IVerificationFactory verificationFactory;

    public StreamVerifiedEncryptionChannelFactory(final IEncryptionFactory encryptionFactory,
            final IVerificationFactory verificationFactory) {
        this.encryptionFactory = encryptionFactory;
        this.verificationFactory = verificationFactory;
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newReader(final ISynchronousReader<IByteBufferProvider> reader) {
        return new StreamVerifiedEncryptionSynchronousReader(reader, encryptionFactory, verificationFactory);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newWriter(final ISynchronousWriter<IByteBufferProvider> writer) {
        return new StreamVerifiedEncryptionSynchronousWriter(writer, encryptionFactory, verificationFactory);
    }

}
