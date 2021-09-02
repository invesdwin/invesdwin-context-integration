package de.invesdwin.context.integration.channel.aeron;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class AeronChannelTest extends AChannelTest {

    private static final AeronMediaDriverMode MODE = AeronMediaDriverMode.EMBEDDED;

    @Test
    public void testAeronDatagramSocketPerformance() throws InterruptedException {
        final String responseChannel = "aeron:udp?endpoint=localhost:7878";
        final String requestChannel = "aeron:udp?endpoint=localhost:7879";
        runAeronPerformanceTest(MODE, responseChannel, 1001, requestChannel, 1002);
    }

    @Test
    public void testAeronIpcPerformance() throws InterruptedException {
        final String responseChannel = "aeron:ipc";
        final String requestChannel = "aeron:ipc";
        runAeronPerformanceTest(MODE, responseChannel, 1001, requestChannel, 1002);
    }

    private void runAeronPerformanceTest(final AeronMediaDriverMode mode, final String responseChannel,
            final int responseStreamId, final String requestChannel, final int requestStreamId)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new AeronSynchronousWriter(mode, responseChannel,
                responseStreamId);
        final ISynchronousReader<IByteBuffer> requestReader = new AeronSynchronousReader(mode, requestChannel,
                requestStreamId);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runAeronPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new AeronSynchronousWriter(mode, requestChannel,
                requestStreamId);
        final ISynchronousReader<IByteBuffer> responseReader = new AeronSynchronousReader(mode, responseChannel,
                responseStreamId);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
