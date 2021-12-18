package de.invesdwin.context.integration.channel.sync.pipe.unsafe;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;

@NotThreadSafe
public class NativePipeChannelTest extends AChannelTest {

    @Test
    public void testNamedPipeNativePerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.PIPE_NATIVE;
        final File requestFile = newFile("testNamedPipePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testNamedPipeNativePerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.PIPE_NATIVE;
        final File requestFile = newFile("testNamedPipePerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

}
