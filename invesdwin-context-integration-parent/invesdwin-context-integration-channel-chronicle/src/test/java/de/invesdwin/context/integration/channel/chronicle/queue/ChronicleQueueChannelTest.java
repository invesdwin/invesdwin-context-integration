package de.invesdwin.context.integration.channel.chronicle.queue;

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
import de.invesdwin.util.time.date.FDate;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

@NotThreadSafe
public class ChronicleQueueChannelTest extends AChannelTest {

    @Test
    public void testLatency() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testChroniclePerformance_request" + SingleChronicleQueue.SUFFIX, tmpfs,
                pipes);
        Files.deleteQuietly(requestFile);
        final File responseFile = newFile("testChroniclePerformance_response" + SingleChronicleQueue.SUFFIX, tmpfs,
                pipes);
        Files.deleteQuietly(responseFile);
        runLatencyTest(requestFile, responseFile);
    }

    @Test
    public void testLatencyWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testChroniclePerformanceWithTmpfs_request" + SingleChronicleQueue.SUFFIX,
                tmpfs, pipes);
        Files.deleteQuietly(requestFile);
        final File responseFile = newFile("testChroniclePerformanceWithTmpfs_response" + SingleChronicleQueue.SUFFIX,
                tmpfs, pipes);
        Files.deleteQuietly(responseFile);
        runLatencyTest(requestFile, responseFile);
    }

    private void runLatencyTest(final File requestFile, final File responseFile) throws InterruptedException {
        try {
            final ISynchronousWriter<IByteBufferProvider> responseWriter = new ChronicleQueueSynchronousWriter(
                    new ChronicleQueueSynchronousChannel(responseFile));
            final ISynchronousReader<IByteBufferProvider> requestReader = new ChronicleQueueSynchronousReader(
                    new ChronicleQueueSynchronousChannel(requestFile));
            final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                    newSerdeWriter(responseWriter));
            final ISynchronousWriter<IByteBufferProvider> requestWriter = new ChronicleQueueSynchronousWriter(
                    new ChronicleQueueSynchronousChannel(requestFile));
            final ISynchronousReader<IByteBufferProvider> responseReader = new ChronicleQueueSynchronousReader(
                    new ChronicleQueueSynchronousChannel(responseFile));
            final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                    newSerdeReader(responseReader));
            new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

    @Test
    public void testChronicleThroughput() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File file = newFile("testChroniclePerformance_file" + SingleChronicleQueue.SUFFIX, tmpfs, pipes);
        Files.deleteQuietly(file);
        runThroughputTest(file);
    }

    @Test
    public void testChronicleThroughputWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File file = newFile("testChroniclePerformanceWithTmpfs_file" + SingleChronicleQueue.SUFFIX, tmpfs, pipes);
        Files.deleteQuietly(file);
        runThroughputTest(file);
    }

    private void runThroughputTest(final File file) throws InterruptedException {
        try {
            final ISynchronousWriter<FDate> channelWriter = newSerdeWriter(
                    new ChronicleQueueSynchronousWriter(new ChronicleQueueSynchronousChannel(file)));
            final ThroughputSenderTask senderTask = new ThroughputSenderTask(this, channelWriter);
            final ISynchronousReader<FDate> channelReader = newSerdeReader(
                    new ChronicleQueueSynchronousReader(new ChronicleQueueSynchronousChannel(file)));
            final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, channelReader);
            new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
        } finally {
            Files.deleteQuietly(file);
        }
    }

}
