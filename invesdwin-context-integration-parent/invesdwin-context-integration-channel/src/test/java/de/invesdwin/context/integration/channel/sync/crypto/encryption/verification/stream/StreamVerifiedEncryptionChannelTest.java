package de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.stream;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.EncryptionChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.verification.VerificationChannelTest;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class StreamVerifiedEncryptionChannelTest extends ALatencyChannelTest {

    public static final IEncryptionFactory ENCRYPTION_FACTORY = EncryptionChannelTest.ENCRYPTION_FACTORY;
    public static final IVerificationFactory VERIFICATION_FACTORY = VerificationChannelTest.VERIFICATION_FACTORY;

    @Test
    public void testStreamVerifiedEncryptionPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testStreamVerifiedEncryptionPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testStreamVerifiedEncryptionPerformance_response.pipe", tmpfs, pipes);
        runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

    @Override
    protected ISynchronousReader<IByteBufferProvider> newReader(final File file, final FileChannelType pipes) {
        return new StreamVerifiedEncryptionSynchronousReader(super.newReader(file, pipes), ENCRYPTION_FACTORY,
                VERIFICATION_FACTORY);
    }

    @Override
    protected ISynchronousWriter<IByteBufferProvider> newWriter(final File file, final FileChannelType pipes) {
        return new StreamVerifiedEncryptionSynchronousWriter(super.newWriter(file, pipes), ENCRYPTION_FACTORY,
                VERIFICATION_FACTORY);
    }

    @Override
    protected int getMaxMessageSize() {
        return 52;
    }

}
