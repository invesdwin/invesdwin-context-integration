package de.invesdwin.context.integration.channel.sync.pipe.unsafe;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;

@NotThreadSafe
public class NativePipeChannelTest extends AChannelTest {

    @Test
    public void testNamedPipeNativeLatency() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.PIPE_NATIVE;
        final File requestFile = newFile("testNamedPipePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformance_response.pipe", tmpfs, pipes);
        new LatencyChannelTest(this).runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testNamedPipeNativeLatencyWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.PIPE_NATIVE;
        final File requestFile = newFile("testNamedPipePerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        new LatencyChannelTest(this).runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testNamedPipeNativeThroughput() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.PIPE_NATIVE;
        final File channelFile = newFile("testNamedPipePerformance_channel.pipe", tmpfs, pipes);
        new ThroughputChannelTest(this).runThroughputTest(pipes, channelFile, null);
    }

    @Test
    public void testNamedPipeNativeThrougputWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.PIPE_NATIVE;
        final File channelFile = newFile("testNamedPipePerformanceWithTmpfs_channel.pipe", tmpfs, pipes);
        new ThroughputChannelTest(this).runThroughputTest(pipes, channelFile, null);
    }

}
