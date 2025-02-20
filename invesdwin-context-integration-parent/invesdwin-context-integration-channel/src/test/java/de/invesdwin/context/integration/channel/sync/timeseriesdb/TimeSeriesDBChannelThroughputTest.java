package de.invesdwin.context.integration.channel.sync.timeseriesdb;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputReceiverTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputSenderTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class TimeSeriesDBChannelThroughputTest extends AChannelTest {

    @Test
    public void testTimeSeriesDBPerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final File requestFolder = newFolder("testTimeSeriesDBPerformance_request", tmpfs);
        Files.cleanDirectoryQuietly(requestFolder);
        final File responseFolder = newFolder("testTimeSeriesDBPerformance_response", tmpfs);
        Files.cleanDirectoryQuietly(responseFolder);
        runTimeSeriesDBPerformanceTest(requestFolder, responseFolder);
    }

    @Test
    public void testTimeSeriesDBPerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final File requestFolder = newFolder("testTimeSeriesDBPerformanceWithTmpfs_request", tmpfs);
        Files.cleanDirectoryQuietly(requestFolder);
        final File responseFolder = newFolder("testTimeSeriesDBPerformanceWithTmpfs_response", tmpfs);
        Files.cleanDirectoryQuietly(responseFolder);
        runTimeSeriesDBPerformanceTest(requestFolder, responseFolder);
    }

    private void runTimeSeriesDBPerformanceTest(final File requestFile, final File responseFile)
            throws InterruptedException {
        try {
            final TimeSeriesDBSynchronousChannel channel = new TimeSeriesDBSynchronousChannel(responseFile,
                    MAX_MESSAGE_SIZE);
            final ISynchronousWriter<IByteBufferProvider> channelWriter = new TimeSeriesDBSynchronousWriter(channel);
            final ThroughputSenderTask senderTask = new ThroughputSenderTask(newSerdeWriter(channelWriter));
            final ISynchronousReader<IByteBufferProvider> channelReader = new TimeSeriesDBSynchronousReader(channel);
            final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, newSerdeReader(channelReader));
            new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

}
