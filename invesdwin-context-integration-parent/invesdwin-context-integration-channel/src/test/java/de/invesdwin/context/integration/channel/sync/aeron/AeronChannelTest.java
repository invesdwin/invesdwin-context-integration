package de.invesdwin.context.integration.channel.sync.aeron;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputReceiverTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputSenderTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class AeronChannelTest extends AChannelTest {

    //https://github.com/real-logic/aeron/blob/master/aeron-driver/src/main/c/README.md
    private static final AeronMediaDriverMode MODE = AeronMediaDriverMode.EMBEDDED;

    @Test
    public void testAeronDatagramSocketLatency() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        //for some more settings to try: https://github.com/real-logic/aeron/wiki/Performance-Testing
        final String responseChannel = "aeron:udp?endpoint=localhost:" + ports[0];
        final String requestChannel = "aeron:udp?endpoint=localhost:" + ports[1];
        //reliability can be turned off https://github.com/real-logic/aeron/issues/541
        //        final String responseChannel = "aeron:udp?control=localhost:"+ ports[0]+"|reliable=false";
        //        final String requestChannel = "aeron:udp?control=localhost:"+ ports[1]+"|reliable=false";
        runAeronLatencyTest(MODE, responseChannel, 1001, requestChannel, 1002);
    }

    @Test
    public void testAeronIpcLatency() throws InterruptedException {
        final String responseChannel = AeronInstance.AERON_IPC_CHANNEL;
        final String requestChannel = AeronInstance.AERON_IPC_CHANNEL;
        runAeronLatencyTest(MODE, responseChannel, 1001, requestChannel, 1002);
    }

    private void runAeronLatencyTest(final AeronMediaDriverMode mode, final String responseChannel,
            final int responseStreamId, final String requestChannel, final int requestStreamId)
            throws InterruptedException {
        final AeronInstance instance = new AeronInstance(mode);
        try {
            final ISynchronousWriter<IByteBufferProvider> responseWriter = new AeronSynchronousWriter(instance,
                    responseChannel, responseStreamId);
            final ISynchronousReader<IByteBufferProvider> requestReader = new AeronSynchronousReader(instance,
                    requestChannel, requestStreamId);
            final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                    newSerdeWriter(responseWriter));
            final ISynchronousWriter<IByteBufferProvider> requestWriter = new AeronSynchronousWriter(instance,
                    requestChannel, requestStreamId);
            final ISynchronousReader<IByteBufferProvider> responseReader = new AeronSynchronousReader(instance,
                    responseChannel, responseStreamId);
            final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                    newSerdeReader(responseReader));
            new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
        } finally {
            Assertions.checkTrue(instance.isClosed());
        }
    }

    @Test
    public void testAeronDatagramSocketThroughput() throws InterruptedException {
        final int port = NetworkUtil.findAvailableUdpPort();
        //for some more settings to try: https://github.com/real-logic/aeron/wiki/Performance-Testing
        final String channel = "aeron:udp?endpoint=localhost:" + port;
        //reliability can be turned off https://github.com/real-logic/aeron/issues/541
        //        final String responseChannel = "aeron:udp?control=localhost:"+ ports[0]+"|reliable=false";
        //        final String requestChannel = "aeron:udp?control=localhost:"+ ports[1]+"|reliable=false";
        runAeronThroughputTest(MODE, channel, 1001);
    }

    @Test
    public void testAeronIpcThroughput() throws InterruptedException {
        final String channel = AeronInstance.AERON_IPC_CHANNEL;
        runAeronThroughputTest(MODE, channel, 1001);
    }

    private void runAeronThroughputTest(final AeronMediaDriverMode mode, final String channel, final int channeltreamId)
            throws InterruptedException {
        final AeronInstance instance = new AeronInstance(mode);
        try {
            final ISynchronousWriter<IByteBufferProvider> senderWriter = new AeronSynchronousWriter(instance, channel,
                    channeltreamId);
            final ThroughputSenderTask senderTask = new ThroughputSenderTask(newSerdeWriter(senderWriter));
            final ISynchronousReader<IByteBufferProvider> receiverReader = new AeronSynchronousReader(instance, channel,
                    channeltreamId);
            final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this,
                    newSerdeReader(receiverReader));
            new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
        } finally {
            Assertions.checkTrue(instance.isClosed());
        }
    }

}
