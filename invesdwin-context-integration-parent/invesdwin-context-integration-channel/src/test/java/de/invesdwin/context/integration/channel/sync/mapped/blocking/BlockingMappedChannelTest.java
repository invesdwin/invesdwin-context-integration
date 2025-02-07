package de.invesdwin.context.integration.channel.sync.mapped.blocking;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;

@NotThreadSafe
public class BlockingMappedChannelTest extends ALatencyChannelTest {

    @Test
    public void testBlockingMappedMemoryPerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.BLOCKING_MAPPED;
        final File requestFile = newFile("testBlockingMappedMemoryPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testBlockingMappedMemoryPerformance_response.pipe", tmpfs, pipes);
        runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testBlockingMappedMemoryPerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.BLOCKING_MAPPED;
        final File requestFile = newFile("testBlockingMappedMemoryPerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testBlockingMappedMemoryPerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

}
