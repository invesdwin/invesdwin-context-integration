package de.invesdwin.context.integration.channel.sync.chronicle.queue;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
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
            final ISynchronousWriter<IByteBufferWriter> responseWriter = new ChronicleQueueSynchronousWriter(
                    responseFile);
            final ISynchronousReader<IByteBuffer> requestReader = new ChronicleQueueSynchronousReader(requestFile);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(responseFile.getName(), 1);
            executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
            final ISynchronousWriter<IByteBufferWriter> requestWriter = new ChronicleQueueSynchronousWriter(
                    requestFile);
            final ISynchronousReader<IByteBuffer> responseReader = new ChronicleQueueSynchronousReader(responseFile);
            read(newCommandWriter(requestWriter), newCommandReader(responseReader));
            executor.shutdown();
            executor.awaitTermination();
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

}
