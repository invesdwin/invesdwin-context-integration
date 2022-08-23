package de.invesdwin.context.integration.channel.sync.aeron;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class AeronChannelTest extends AChannelTest {

    //https://github.com/real-logic/aeron/blob/master/aeron-driver/src/main/c/README.md
    private static final AeronMediaDriverMode MODE = AeronMediaDriverMode.EMBEDDED;

    @Test
    public void testAeronDatagramSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        //for some more settings to try: https://github.com/real-logic/aeron/wiki/Performance-Testing
        final String responseChannel = "aeron:udp?endpoint=localhost:" + ports[0];
        final String requestChannel = "aeron:udp?endpoint=localhost:" + ports[1];
        //reliability can be turned off https://github.com/real-logic/aeron/issues/541
        //        final String responseChannel = "aeron:udp?control=localhost:"+ ports[0]+"|reliable=false";
        //        final String requestChannel = "aeron:udp?control=localhost:"+ ports[1]+"|reliable=false";
        runAeronPerformanceTest(MODE, responseChannel, 1001, requestChannel, 1002);
    }

    @Test
    public void testAeronIpcPerformance() throws InterruptedException {
        final String responseChannel = AeronInstance.AERON_IPC_CHANNEL;
        final String requestChannel = AeronInstance.AERON_IPC_CHANNEL;
        runAeronPerformanceTest(MODE, responseChannel, 1001, requestChannel, 1002);
    }

    private void runAeronPerformanceTest(final AeronMediaDriverMode mode, final String responseChannel,
            final int responseStreamId, final String requestChannel, final int requestStreamId)
            throws InterruptedException {
        final AeronInstance instance = new AeronInstance(mode);
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new AeronSynchronousWriter(instance,
                responseChannel, responseStreamId);
        final ISynchronousReader<IByteBuffer> requestReader = new AeronSynchronousReader(instance, requestChannel,
                requestStreamId);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runAeronPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new AeronSynchronousWriter(instance, requestChannel,
                requestStreamId);
        final ISynchronousReader<IByteBuffer> responseReader = new AeronSynchronousReader(instance, responseChannel,
                responseStreamId);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
        Assertions.checkTrue(instance.isClosed());
    }

}
