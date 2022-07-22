package de.invesdwin.context.integration.channel.sync.crypto.encryption.authentication.stream;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.authentication.AuthenticationChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.EncryptionChannelTest;
import de.invesdwin.context.security.crypto.authentication.IAuthenticationFactory;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class StreamAuthenticatedEncryptionChannelTest extends AChannelTest {

    public static final IEncryptionFactory ENCRYPTION_FACTORY = EncryptionChannelTest.ENCRYPTION_FACTORY;
    public static final IAuthenticationFactory AUTHENTICATION_FACTORY = AuthenticationChannelTest.AUTHENTICATION_FACTORY;

    @Test
    public void testEncryptionPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testStreamAuthenticatedEncryptionPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testStreamAuthenticatedEncryptionPerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Override
    protected ISynchronousReader<IByteBuffer> newReader(final File file, final FileChannelType pipes) {
        return new StreamAuthenticatedEncryptionSynchronousReader(super.newReader(file, pipes), ENCRYPTION_FACTORY,
                AUTHENTICATION_FACTORY);
    }

    @Override
    protected ISynchronousWriter<IByteBufferWriter> newWriter(final File file, final FileChannelType pipes) {
        return new StreamAuthenticatedEncryptionSynchronousWriter(super.newWriter(file, pipes), ENCRYPTION_FACTORY,
                AUTHENTICATION_FACTORY);
    }

    @Override
    protected int getMaxMessageSize() {
        return 52;
    }

}
