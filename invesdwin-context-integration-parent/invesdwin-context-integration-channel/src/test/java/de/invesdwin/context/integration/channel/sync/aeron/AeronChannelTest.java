package de.invesdwin.context.integration.channel.sync.aeron;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class AeronChannelTest extends ALatencyChannelTest {

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
        try {
            final ISynchronousWriter<IByteBufferProvider> responseWriter = new AeronSynchronousWriter(instance,
                    responseChannel, responseStreamId);
            final ISynchronousReader<IByteBufferProvider> requestReader = new AeronSynchronousReader(instance,
                    requestChannel, requestStreamId);
            final LatencyServerTask serverTask = new LatencyServerTask(newSerdeReader(requestReader),
                    newSerdeWriter(responseWriter));
            final ISynchronousWriter<IByteBufferProvider> requestWriter = new AeronSynchronousWriter(instance,
                    requestChannel, requestStreamId);
            final ISynchronousReader<IByteBufferProvider> responseReader = new AeronSynchronousReader(instance,
                    responseChannel, responseStreamId);
            final LatencyClientTask clientTask = new LatencyClientTask(newSerdeWriter(requestWriter),
                    newSerdeReader(responseReader));
            runLatencyTest(serverTask, clientTask);
        } finally {
            Assertions.checkTrue(instance.isClosed());
        }
    }

}
