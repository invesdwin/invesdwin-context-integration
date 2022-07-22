package de.invesdwin.context.integration.channel.sync.crypto.authentication;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.encryption.EncryptionChannelTest;
import de.invesdwin.context.security.crypto.authentication.IAuthenticationFactory;
import de.invesdwin.context.security.crypto.authentication.mac.MacAuthenticationFactory;
import de.invesdwin.context.security.crypto.key.IDerivedKeyProvider;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class AuthenticationChannelTest extends AChannelTest {

    public static final IDerivedKeyProvider DERIVED_KEY_PROVIDER = EncryptionChannelTest.DERIVED_KEY_PROVIDER;
    public static final IAuthenticationFactory AUTHENTICATION_FACTORY = new MacAuthenticationFactory(
            DERIVED_KEY_PROVIDER);

    @Test
    public void testAuthenticationPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testAuthenticationPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testAuthenticationPerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Override
    protected ISynchronousReader<IByteBuffer> newReader(final File file, final FileChannelType pipes) {
        return new AuthenticationSynchronousReader(super.newReader(file, pipes), AUTHENTICATION_FACTORY);
    }

    @Override
    protected ISynchronousWriter<IByteBufferWriter> newWriter(final File file, final FileChannelType pipes) {
        return new AuthenticationSynchronousWriter(super.newWriter(file, pipes), AUTHENTICATION_FACTORY);
    }

    @Override
    protected int getMaxMessageSize() {
        return super.getMaxMessageSize() + AUTHENTICATION_FACTORY.getAlgorithm().getMacLength();
    }

}
