package de.invesdwin.context.integration.channel.sync.nng;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.nng.type.INngSocketType;
import de.invesdwin.context.integration.channel.sync.nng.type.NngSocketType;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Requires: apt install libnanomsg5
 *
 * @author subes
 *
 */
@NotThreadSafe
public class NngChannelTest extends AChannelTest {

    @Test
    public void testNngTcpPairPerformance() throws InterruptedException {
        final String responseChannel = "tcp://127.0.0.1:7878";
        final String requestChannel = "tcp://127.0.0.1:7879";
        runNngPerformanceTest(NngSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testNngTcpPushPullPerformance() throws InterruptedException {
        final String responseChannel = "tcp://127.0.0.1:7878";
        final String requestChannel = "tcp://127.0.0.1:7879";
        runNngPerformanceTest(NngSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testNngIpcPairPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runNngPerformanceTest(NngSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testNngIpcPushPullPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runNngPerformanceTest(NngSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testNngInprocPairPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runNngPerformanceTest(NngSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testNngInprocPushPullPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runNngPerformanceTest(NngSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testNngInprocPubSubPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runNngPerformanceTest(NngSocketType.PUBSUB, responseChannel, requestChannel);
    }

    private void runNngPerformanceTest(final INngSocketType socketType, final String responseChannel,
            final String requestChannel) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NngSynchronousWriter(socketType,
                responseChannel, true);
        final ISynchronousReader<IByteBufferProvider> requestReader = new NngSynchronousReader(socketType,
                requestChannel, true);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNngPerformanceTest", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NngSynchronousWriter(socketType,
                requestChannel, false);
        final ISynchronousReader<IByteBufferProvider> responseReader = new NngSynchronousReader(socketType,
                responseChannel, false);
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

}
