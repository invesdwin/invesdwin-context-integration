package de.invesdwin.context.integration.channel.jeromq;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.zeromq.JeromqSynchronousReader;
import de.invesdwin.context.integration.channel.zeromq.JeromqSynchronousWriter;
import de.invesdwin.context.integration.channel.zeromq.type.IJeromqSocketType;
import de.invesdwin.context.integration.channel.zeromq.type.JeromqSocketType;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class JeromqChannelTest extends AChannelTest {

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
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new JeromqSynchronousWriter(socketType,
                responseChannel, true);
        final ISynchronousReader<IByteBuffer> requestReader = new JeromqSynchronousReader(socketType, requestChannel,
                true);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runJeromqPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new JeromqSynchronousWriter(socketType,
                requestChannel, false);
        final ISynchronousReader<IByteBuffer> responseReader = new JeromqSynchronousReader(socketType, responseChannel,
                false);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}