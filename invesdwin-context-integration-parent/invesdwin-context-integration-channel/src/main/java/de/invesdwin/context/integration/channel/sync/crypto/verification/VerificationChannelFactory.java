package de.invesdwin.context.integration.channel.sync.crypto.verification;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class VerificationChannelFactory
        implements ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> {

    private final IVerificationFactory verificationFactory;

    public VerificationChannelFactory(final IVerificationFactory verificationFactory) {
        this.verificationFactory = verificationFactory;
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newReader(final ISynchronousReader<IByteBufferProvider> reader) {
        return new VerificationSynchronousReader(reader, verificationFactory);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newWriter(final ISynchronousWriter<IByteBufferProvider> writer) {
        return new VerificationSynchronousWriter(writer, verificationFactory);
    }

}
