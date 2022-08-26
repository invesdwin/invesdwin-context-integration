package de.invesdwin.context.integration.channel.sync.crypto.verification;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.EncryptionChannelTest;
import de.invesdwin.context.security.crypto.key.IDerivedKeyProvider;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.context.security.crypto.verification.hash.HashVerificationFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class VerificationChannelTest extends AChannelTest {

    public static final IDerivedKeyProvider DERIVED_KEY_PROVIDER = EncryptionChannelTest.DERIVED_KEY_PROVIDER;
    public static final IVerificationFactory VERIFICATION_FACTORY = new HashVerificationFactory(DERIVED_KEY_PROVIDER);

    @Test
    public void testVerificationPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testVerificationPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testVerificationPerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Override
    protected ISynchronousReader<IByteBufferProvider> newReader(final File file, final FileChannelType pipes) {
        return new VerificationSynchronousReader(super.newReader(file, pipes), VERIFICATION_FACTORY);
    }

    @Override
    protected ISynchronousWriter<IByteBufferProvider> newWriter(final File file, final FileChannelType pipes) {
        return new VerificationSynchronousWriter(super.newWriter(file, pipes), VERIFICATION_FACTORY);
    }

    @Override
    protected int getMaxMessageSize() {
        return super.getMaxMessageSize() + VERIFICATION_FACTORY.getAlgorithm().getHashSize();
    }

}
