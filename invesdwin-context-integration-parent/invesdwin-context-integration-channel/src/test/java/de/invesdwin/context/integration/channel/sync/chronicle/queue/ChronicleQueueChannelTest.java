package de.invesdwin.context.integration.channel.sync.chronicle.queue;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

@NotThreadSafe
public class ChronicleQueueChannelTest extends AChannelTest {

    @Test
    public void testChroniclePerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testChroniclePerformance_request" + SingleChronicleQueue.SUFFIX, tmpfs,
                pipes);
        Files.deleteQuietly(requestFile);
        final File responseFile = newFile("testChroniclePerformance_response" + SingleChronicleQueue.SUFFIX, tmpfs,
                pipes);
        Files.deleteQuietly(responseFile);
        runChroniclePerformanceTest(requestFile, responseFile);
    }

    @Test
    public void testChroniclePerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testChroniclePerformanceWithTmpfs_request" + SingleChronicleQueue.SUFFIX,
                tmpfs, pipes);
        Files.deleteQuietly(requestFile);
        final File responseFile = newFile("testChroniclePerformanceWithTmpfs_response" + SingleChronicleQueue.SUFFIX,
                tmpfs, pipes);
        Files.deleteQuietly(responseFile);
        runChroniclePerformanceTest(requestFile, responseFile);
    }

    private void runChroniclePerformanceTest(final File requestFile, final File responseFile)
            throws InterruptedException {
        try {
            final ISynchronousWriter<IByteBufferProvider> responseWriter = new ChronicleQueueSynchronousWriter(
                    responseFile);
            final ISynchronousReader<IByteBufferProvider> requestReader = new ChronicleQueueSynchronousReader(
                    requestFile);
            final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                    newSerdeWriter(responseWriter));
            final ISynchronousWriter<IByteBufferProvider> requestWriter = new ChronicleQueueSynchronousWriter(
                    requestFile);
            final ISynchronousReader<IByteBufferProvider> responseReader = new ChronicleQueueSynchronousReader(
                    responseFile);
            final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                    newSerdeReader(responseReader));
            new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

}
