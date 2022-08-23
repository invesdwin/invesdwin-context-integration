package de.invesdwin.context.integration.channel.sync.crypto.encryption.verification;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.EncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.verification.VerificationChannelTest;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class VerifiedEncryptionChannelTest extends AChannelTest {

    public static final IEncryptionFactory ENCRYPTION_FACTORY = EncryptionChannelTest.ENCRYPTION_FACTORY;
    public static final IVerificationFactory VERIFICATION_FACTORY = VerificationChannelTest.VERIFICATION_FACTORY;

    @Test
    public void testVerifiedEncryptionPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testVerifiedEncryptionPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testVerifiedEncryptionPerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Override
    protected ISynchronousReader<IByteBuffer> newReader(final File file, final FileChannelType pipes) {
        return new VerifiedEncryptionSynchronousReader(super.newReader(file, pipes), ENCRYPTION_FACTORY,
                VERIFICATION_FACTORY);
    }

    @Override
    protected ISynchronousWriter<IByteBufferProvider> newWriter(final File file, final FileChannelType pipes) {
        return new VerifiedEncryptionSynchronousWriter(super.newWriter(file, pipes), ENCRYPTION_FACTORY,
                VERIFICATION_FACTORY);
    }

    @Override
    protected int getMaxMessageSize() {
        return 56;
    }

}
