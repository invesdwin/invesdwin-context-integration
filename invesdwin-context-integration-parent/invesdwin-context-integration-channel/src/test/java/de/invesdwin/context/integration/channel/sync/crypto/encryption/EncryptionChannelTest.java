package de.invesdwin.context.integration.channel.sync.crypto.encryption;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.encryption.cipher.symmetric.ISymmetricCipherAlgorithm;
import de.invesdwin.context.security.crypto.encryption.cipher.symmetric.SymmetricEncryptionFactory;
import de.invesdwin.context.security.crypto.key.DerivedKeyProvider;
import de.invesdwin.context.security.crypto.random.CryptoRandomGenerator;
import de.invesdwin.context.security.crypto.random.CryptoRandomGenerators;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class EncryptionChannelTest extends AChannelTest {

    public static final DerivedKeyProvider DERIVED_KEY_PROVIDER;
    public static final IEncryptionFactory ENCRYPTION_FACTORY;

    static {
        final CryptoRandomGenerator random = CryptoRandomGenerators.getThreadLocalCryptoRandom();
        final byte[] key = ByteBuffers
                .allocateByteArray(ISymmetricCipherAlgorithm.getDefault().getDefaultKeySizeBits() / Byte.SIZE);
        //keep the key constant between tests to ease debugging
        if (!DEBUG) {
            random.nextBytes(key);
        }
        DERIVED_KEY_PROVIDER = DerivedKeyProvider.fromRandom(EncryptionChannelTest.class.getSimpleName().getBytes(),
                key);
        ENCRYPTION_FACTORY = new SymmetricEncryptionFactory(DERIVED_KEY_PROVIDER);
    }

    @Test
    public void testEncryptionPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testEncryptionPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testEncryptionPerformance_response.pipe", tmpfs, pipes);
        new LatencyChannelTest(this).runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newReader(final File file, final FileChannelType pipes) {
        return new EncryptionSynchronousReader(super.newReader(file, pipes), ENCRYPTION_FACTORY);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newWriter(final File file, final FileChannelType pipes) {
        return new EncryptionSynchronousWriter(super.newWriter(file, pipes), ENCRYPTION_FACTORY);
    }

    @Override
    public int getMaxMessageSize() {
        return 24;
    }

}
