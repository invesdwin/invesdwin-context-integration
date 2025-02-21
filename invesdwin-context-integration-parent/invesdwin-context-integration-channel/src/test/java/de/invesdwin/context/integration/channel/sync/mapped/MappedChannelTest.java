package de.invesdwin.context.integration.channel.sync.mapped;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;

@NotThreadSafe
public class MappedChannelTest extends AChannelTest {

    @Test
    public void testMappedMemoryLatency() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testMappedMemoryLatency_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryLatency_response.pipe", tmpfs, pipes);
        new LatencyChannelTest(this).runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testMappedMemoryLatencyWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testMappedMemoryLatencyyWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryLatencyWithTmpfs_response.pipe", tmpfs, pipes);
        new LatencyChannelTest(this).runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testMappedMemoryThroughput() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File channelFile = newFile("testMappedMemoryThroughput_channel.pipe", tmpfs, pipes);
        new ThroughputChannelTest(this).runThroughputTest(pipes, channelFile, null);
    }

    @Test
    public void testMappedMemoryThroughputWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File channelFile = newFile("testMappedMemoryThroughputWithTmpfs_channel.pipe", tmpfs, pipes);
        new ThroughputChannelTest(this).runThroughputTest(pipes, channelFile, null);
    }

}
