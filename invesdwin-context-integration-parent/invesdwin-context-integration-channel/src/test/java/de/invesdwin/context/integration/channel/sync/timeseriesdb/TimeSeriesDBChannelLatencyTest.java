package de.invesdwin.context.integration.channel.sync.timeseriesdb;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class TimeSeriesDBChannelLatencyTest extends ALatencyChannelTest {

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
            final TimeSeriesDBSynchronousChannel requestChannel = new TimeSeriesDBSynchronousChannel(requestFile,
                    MAX_MESSAGE_SIZE);
            final TimeSeriesDBSynchronousChannel responseChannel = new TimeSeriesDBSynchronousChannel(responseFile,
                    MAX_MESSAGE_SIZE);
            final ISynchronousWriter<IByteBufferProvider> responseWriter = new TimeSeriesDBSynchronousWriter(
                    responseChannel);
            final ISynchronousReader<IByteBufferProvider> requestReader = new TimeSeriesDBSynchronousReader(
                    requestChannel);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(responseFile.getName(), 1);
            executor.execute(new LatencyServerTask(newSerdeReader(requestReader), newSerdeWriter(responseWriter)));
            final ISynchronousWriter<IByteBufferProvider> requestWriter = new TimeSeriesDBSynchronousWriter(
                    requestChannel);
            final ISynchronousReader<IByteBufferProvider> responseReader = new TimeSeriesDBSynchronousReader(
                    responseChannel);
            new LatencyClientTask(newSerdeWriter(requestWriter), newSerdeReader(responseReader)).run();
            executor.shutdown();
            executor.awaitTermination();
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

}
