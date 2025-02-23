package de.invesdwin.context.integration.channel.sync.batch;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class BatchChannelTest extends AChannelTest {

    @Test
    public void testBatchPerformance() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.BLOCKING_MAPPED;
        final File channelFile = newFile("testBatchPerformance_channel.pipe", tmpfs, pipes);
        new ThroughputChannelTest(this).runThroughputTest(pipes, channelFile, null);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newReader(final File file, final FileChannelType pipes) {
        return new BatchSynchronousReader(super.newReader(file, pipes));
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newWriter(final File file, final FileChannelType pipes) {
        return new BatchSynchronousWriter(super.newWriter(file, pipes), getMaxMessageSize(), 2, Duration.ONE_MINUTE);
    }

    @Override
    public int getMaxMessageSize() {
        return 1000;
    }

}
