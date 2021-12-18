package de.invesdwin.context.integration.channel.sync.aeron;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class AeronChannelTest extends AChannelTest {

    //https://github.com/real-logic/aeron/blob/master/aeron-driver/src/main/c/README.md
    private static final AeronMediaDriverMode MODE = AeronMediaDriverMode.EMBEDDED;

    @Test
    public void testAeronDatagramSocketPerformance() throws InterruptedException {
        //for some more settings to try: https://github.com/real-logic/aeron/wiki/Performance-Testing
        final String responseChannel = "aeron:udp?endpoint=localhost:7878";
        final String requestChannel = "aeron:udp?endpoint=localhost:7879";
        //reliability can be turned off https://github.com/real-logic/aeron/issues/541
        //        final String responseChannel = "aeron:udp?control=localhost:7878|reliable=false";
        //        final String requestChannel = "aeron:udp?control=localhost:7879|reliable=false";
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
