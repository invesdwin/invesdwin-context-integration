package de.invesdwin.context.integration.channel.sync.crypto.encryption;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.encryption.cipher.CipherEncryptionFactory;
import de.invesdwin.context.security.crypto.encryption.cipher.aes.AesKeyLength;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.random.CryptoRandomGenerator;
import de.invesdwin.context.security.crypto.random.CryptoRandomGeneratorObjectPool;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class EncryptionChannelTest extends AChannelTest {

    public static final IEncryptionFactory CRYPTO_FACTORY;

    static {
        final CryptoRandomGenerator random = CryptoRandomGeneratorObjectPool.INSTANCE.borrowObject();
        try {
            final byte[] key = ByteBuffers.allocateByteArray(AesKeyLength._256.getBytes());
            final DerivedKeyProvider derivedKeyProvider = DerivedKeyProvider
                    .fromRandom(EncryptionChannelTest.class.getSimpleName().getBytes(), key);
            CRYPTO_FACTORY = new CipherEncryptionFactory(derivedKeyProvider);
        } finally {
            CryptoRandomGeneratorObjectPool.INSTANCE.returnObject(random);
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
        return 24;
    }

}
