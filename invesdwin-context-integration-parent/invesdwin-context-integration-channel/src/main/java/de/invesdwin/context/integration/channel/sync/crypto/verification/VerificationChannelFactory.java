package de.invesdwin.context.integration.channel.sync.crypto.verification;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@Immutable
public class VerificationChannelFactory implements ISynchronousChannelFactory<IByteBuffer, IByteBufferWriter> {

    private final IVerificationFactory verificationFactory;

    public VerificationChannelFactory(final IVerificationFactory verificationFactory) {
        this.verificationFactory = verificationFactory;
    }

    @Override
    public ISynchronousReader<IByteBuffer> newReader(final ISynchronousReader<IByteBuffer> reader) {
        return new VerificationSynchronousReader(reader, verificationFactory);
    }

    @Override
    public ISynchronousWriter<IByteBufferWriter> newWriter(final ISynchronousWriter<IByteBufferWriter> writer) {
        return new VerificationSynchronousWriter(writer, verificationFactory);
    }

}
