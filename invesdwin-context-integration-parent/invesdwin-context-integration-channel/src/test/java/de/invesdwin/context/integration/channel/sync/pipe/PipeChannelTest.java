package de.invesdwin.context.integration.channel.sync.pipe;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;

@NotThreadSafe
public class PipeChannelTest extends ALatencyChannelTest {

    @Test
    public void testNamedPipePerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.PIPE;
        final File requestFile = newFile("testNamedPipePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformance_response.pipe", tmpfs, pipes);
        runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testNamedPipePerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.PIPE;
        final File requestFile = newFile("testNamedPipePerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runLatencyTest(pipes, requestFile, responseFile, null, null);
    }

}
