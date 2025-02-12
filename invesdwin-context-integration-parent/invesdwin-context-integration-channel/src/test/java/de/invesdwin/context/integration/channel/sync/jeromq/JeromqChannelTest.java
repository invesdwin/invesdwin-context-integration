package de.invesdwin.context.integration.channel.sync.jeromq;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jeromq.type.IJeromqSocketType;
import de.invesdwin.context.integration.channel.sync.jeromq.type.JeromqSocketType;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class JeromqChannelTest extends ALatencyChannelTest {

    @Test
    public void testJeromqTcpPairPerformance() throws InterruptedException {
        final String responseChannel = "tcp://localhost:7878";
        final String requestChannel = "tcp://localhost:7879";
        runJeromqPerformanceTest(JeromqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqTcpPushPullPerformance() throws InterruptedException {
        final String responseChannel = "tcp://localhost:7878";
        final String requestChannel = "tcp://localhost:7879";
        runJeromqPerformanceTest(JeromqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqIpcPairPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runJeromqPerformanceTest(JeromqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqIpcPushPullPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runJeromqPerformanceTest(JeromqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqInprocPairPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJeromqPerformanceTest(JeromqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqInprocPushPullPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJeromqPerformanceTest(JeromqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqInprocPubSubPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJeromqPerformanceTest(JeromqSocketType.PUBSUB, responseChannel, requestChannel);
    }

    private void runJeromqPerformanceTest(final IJeromqSocketType socketType, final String responseChannel,
            final String requestChannel) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new JeromqSynchronousWriter(socketType,
                responseChannel, true);
        final ISynchronousReader<IByteBufferProvider> requestReader = new JeromqSynchronousReader(socketType,
                requestChannel, true);
        final LatencyServerTask serverTask = new LatencyServerTask(newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new JeromqSynchronousWriter(socketType,
                requestChannel, false);
        final ISynchronousReader<IByteBufferProvider> responseReader = new JeromqSynchronousReader(socketType,
                responseChannel, false);
        final LatencyClientTask clientTask = new LatencyClientTask(newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        runLatencyTest(serverTask, clientTask);
    }

}
