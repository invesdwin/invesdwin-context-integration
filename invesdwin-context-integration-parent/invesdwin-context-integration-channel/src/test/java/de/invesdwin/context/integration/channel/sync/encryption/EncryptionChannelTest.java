package de.invesdwin.context.integration.channel.sync.encryption;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.streams.encryption.aes.AesAlgorithm;
import de.invesdwin.context.integration.streams.encryption.aes.AesEncryptionFactory;
import de.invesdwin.context.integration.streams.encryption.aes.AesKeyLength;
import de.invesdwin.context.integration.streams.encryption.random.CryptoRandomGenerator;
import de.invesdwin.context.integration.streams.encryption.random.CryptoRandomGenerators;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class EncryptionChannelTest extends AChannelTest {

    public static final AesEncryptionFactory CRYPTO_FACTORY;

    static {
        try (CryptoRandomGenerator random = CryptoRandomGenerators.newSecureRandom()) {
            final byte[] key = ByteBuffers.allocateByteArray(AesKeyLength._256.getBytes());
            random.nextBytes(key);
            CRYPTO_FACTORY = new AesEncryptionFactory(AesAlgorithm.DEFAULT, key);
        }
    }

    @Test
    public void testEncryptionPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testEncryptionPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testEncryptionPerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Override
    protected ISynchronousReader<IByteBuffer> newReader(final File file, final FileChannelType pipes) {
        return new EncryptionSynchronousReader(super.newReader(file, pipes), CRYPTO_FACTORY);
    }

    @Override
    protected ISynchronousWriter<IByteBufferWriter> newWriter(final File file, final FileChannelType pipes) {
        return new EncryptionSynchronousWriter(super.newWriter(file, pipes), CRYPTO_FACTORY);
    }

    @Override
    protected int getMaxMessageSize() {
        final int size = super.getMaxMessageSize() + CRYPTO_FACTORY.getAlgorithm().getIvBytes();
        return size;
    }

}
