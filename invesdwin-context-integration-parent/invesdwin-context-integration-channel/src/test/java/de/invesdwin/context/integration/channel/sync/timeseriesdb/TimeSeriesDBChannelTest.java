package de.invesdwin.context.integration.channel.sync.timeseriesdb;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputReceiverTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputSenderTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class TimeSeriesDBChannelTest extends AChannelTest {

    @Test
    public void testTimeSeriesDBLatency() throws InterruptedException {
        final boolean tmpfs = false;
        final File requestFolder = newFolder("testTimeSeriesDBLatency_request", tmpfs);
        Files.cleanDirectoryQuietly(requestFolder);
        final File responseFolder = newFolder("testTimeSeriesDBLatency_response", tmpfs);
        Files.cleanDirectoryQuietly(responseFolder);
        runTimeSeriesDBLatencyTest(requestFolder, responseFolder);
    }

    @Test
    public void testTimeSeriesDBLatencyWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final File requestFolder = newFolder("testTimeSeriesDBLatencyWithTmpfs_request", tmpfs);
        Files.cleanDirectoryQuietly(requestFolder);
        final File responseFolder = newFolder("testTimeSeriesDBLatencyWithTmpfs_response", tmpfs);
        Files.cleanDirectoryQuietly(responseFolder);
        runTimeSeriesDBLatencyTest(requestFolder, responseFolder);
    }

    private void runTimeSeriesDBLatencyTest(final File requestFile, final File responseFile)
            throws InterruptedException {
        try {
            final TimeSeriesDBSynchronousChannel requestChannel = new TimeSeriesDBSynchronousChannel(requestFile,
                    MAX_MESSAGE_SIZE);
            final TimeSeriesDBSynchronousChannel responseChannel = new TimeSeriesDBSynchronousChannel(responseFile,
                    MAX_MESSAGE_SIZE);
            final ISynchronousWriter<IByteBufferProvider> responseWriter = new TimeSeriesDBSynchronousWriter(
                    responseChannel);
            final ISynchronousReader<IByteBufferProvider> requestReader = new TimeSeriesDBSynchronousReader(
                    requestChannel);
            final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                    newSerdeWriter(responseWriter));
            final ISynchronousWriter<IByteBufferProvider> requestWriter = new TimeSeriesDBSynchronousWriter(
                    requestChannel);
            final ISynchronousReader<IByteBufferProvider> responseReader = new TimeSeriesDBSynchronousReader(
                    responseChannel);
            final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                    newSerdeReader(responseReader));
            new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

    @Test
    public void testTimeSeriesDBThroughput() throws InterruptedException {
        final boolean tmpfs = false;
        final File requestFolder = newFolder("testTimeSeriesDBThroughput_request", tmpfs);
        Files.cleanDirectoryQuietly(requestFolder);
        final File responseFolder = newFolder("testTimeSeriesDBThroughput_response", tmpfs);
        Files.cleanDirectoryQuietly(responseFolder);
        runTimeSeriesDBThroughputTest(requestFolder, responseFolder);
    }

    @Test
    public void testTimeSeriesDBThroughputWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final File requestFolder = newFolder("testTimeSeriesDBThroughputWithTmpfs_request", tmpfs);
        Files.cleanDirectoryQuietly(requestFolder);
        final File responseFolder = newFolder("testTimeSeriesDBThroughputWithTmpfs_response", tmpfs);
        Files.cleanDirectoryQuietly(responseFolder);
        runTimeSeriesDBThroughputTest(requestFolder, responseFolder);
    }

    private void runTimeSeriesDBThroughputTest(final File requestFile, final File responseFile)
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
