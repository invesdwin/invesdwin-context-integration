package de.invesdwin.context.integration.channel.sync.compression;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class CompressionChannelTest extends AChannelTest {

    @Test
    public void testCompressionPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testCompressionPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testCompressionPerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Override
    protected ISynchronousReader<IByteBufferProvider> newReader(final File file, final FileChannelType pipes) {
        return new CompressionSynchronousReader(super.newReader(file, pipes));
    }

    @Override
    protected ISynchronousWriter<IByteBufferProvider> newWriter(final File file, final FileChannelType pipes) {
        return new CompressionSynchronousWriter(super.newWriter(file, pipes));
    }

    @Override
    protected int getMaxMessageSize() {
        return 13;
    }

}
