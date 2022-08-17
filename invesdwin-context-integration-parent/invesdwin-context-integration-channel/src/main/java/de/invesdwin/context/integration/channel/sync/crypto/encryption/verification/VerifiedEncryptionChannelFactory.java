package de.invesdwin.context.integration.channel.sync.crypto.encryption.verification;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@Immutable
public class VerifiedEncryptionChannelFactory implements ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> {

    private final IEncryptionFactory encryptionFactory;
    private final IVerificationFactory verificationFactory;

    public VerifiedEncryptionChannelFactory(final IEncryptionFactory encryptionFactory,
            final IVerificationFactory verificationFactory) {
        this.encryptionFactory = encryptionFactory;
        this.verificationFactory = verificationFactory;
    }

    @Override
    public ISynchronousReader<IByteBuffer> newReader(final ISynchronousReader<IByteBuffer> reader) {
        return new VerifiedEncryptionSynchronousReader(reader, encryptionFactory, verificationFactory);
    }

    @Override
    public ISynchronousWriter<IByteBufferWriter> newWriter(final ISynchronousWriter<IByteBufferWriter> writer) {
        return new VerifiedEncryptionSynchronousWriter(writer, encryptionFactory, verificationFactory);
    }

}
