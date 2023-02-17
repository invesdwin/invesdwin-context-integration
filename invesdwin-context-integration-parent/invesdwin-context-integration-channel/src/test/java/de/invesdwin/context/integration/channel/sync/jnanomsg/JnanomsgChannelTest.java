package de.invesdwin.context.integration.channel.sync.jnanomsg;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jnanomsg.type.IJnanomsgSocketType;
import de.invesdwin.context.integration.channel.sync.jnanomsg.type.JnanomsgSocketType;
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
public class JnanomsgChannelTest extends AChannelTest {

    @Test
    public void testJnanomsgTcpPairPerformance() throws InterruptedException {
        final String responseChannel = "tcp://127.0.0.1:7878";
        final String requestChannel = "tcp://127.0.0.1:7879";
        runJnanomsgPerformanceTest(JnanomsgSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJnanomsgTcpPushPullPerformance() throws InterruptedException {
        final String responseChannel = "tcp://127.0.0.1:7878";
        final String requestChannel = "tcp://127.0.0.1:7879";
        runJnanomsgPerformanceTest(JnanomsgSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJnanomsgIpcPairPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runJnanomsgPerformanceTest(JnanomsgSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJnanomsgIpcPushPullPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runJnanomsgPerformanceTest(JnanomsgSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJnanomsgInprocPairPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJnanomsgPerformanceTest(JnanomsgSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJnanomsgInprocPushPullPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJnanomsgPerformanceTest(JnanomsgSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJnanomsgInprocPubSubPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJnanomsgPerformanceTest(JnanomsgSocketType.PUBSUB, responseChannel, requestChannel);
    }

    private void runJnanomsgPerformanceTest(final IJnanomsgSocketType socketType, final String responseChannel,
            final String requestChannel) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new JnanomsgSynchronousWriter(socketType,
                responseChannel, true);
        final ISynchronousReader<IByteBufferProvider> requestReader = new JnanomsgSynchronousReader(socketType,
                requestChannel, true);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runJnanomsgPerformanceTest", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new JnanomsgSynchronousWriter(socketType,
                requestChannel, false);
        final ISynchronousReader<IByteBufferProvider> responseReader = new JnanomsgSynchronousReader(socketType,
                responseChannel, false);
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

}
