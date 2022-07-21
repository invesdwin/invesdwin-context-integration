package de.invesdwin.context.integration.channel.sync.encryption.stream;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.encryption.EncryptionChannelTest;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class StreamEncryptionChannelTest extends AChannelTest {

    public static final IEncryptionFactory CRYPTO_FACTORY = EncryptionChannelTest.CRYPTO_FACTORY;

    @Test
    public void testEncryptionPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testStreamEncryptionPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testStreamEncryptionPerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Override
    protected ISynchronousReader<IByteBuffer> newReader(final File file, final FileChannelType pipes) {
        return new StreamEncryptionSynchronousReader(super.newReader(file, pipes), CRYPTO_FACTORY);
    }

    @Override
    protected ISynchronousWriter<IByteBufferWriter> newWriter(final File file, final FileChannelType pipes) {
        return new StreamEncryptionSynchronousWriter(super.newWriter(file, pipes), CRYPTO_FACTORY);
    }

    @Override
    protected int getMaxMessageSize() {
        return 28;
    }

}
