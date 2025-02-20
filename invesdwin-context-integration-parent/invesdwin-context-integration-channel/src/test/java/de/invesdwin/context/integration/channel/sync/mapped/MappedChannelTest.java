package de.invesdwin.context.integration.channel.sync.mapped;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;

@NotThreadSafe
public class MappedChannelTest extends AChannelTest {

    @Test
    public void testMappedMemoryPerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testMappedMemoryPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryPerformance_response.pipe", tmpfs, pipes);
        new LatencyChannelTest(this).runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testMappedMemoryPerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testMappedMemoryPerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryPerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        new LatencyChannelTest(this).runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

}
