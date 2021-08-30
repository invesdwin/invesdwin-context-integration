package de.invesdwin.context.integration.channel.jocket;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

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
public class JocketChannelTest extends AChannelTest {

    @Test
    public void testJocketPerformance() throws InterruptedException, IOException {
        final JocketChannel server = new JocketChannel(6565, true, MESSAGE_SIZE);
        final JocketChannel client = new JocketChannel(6565, false, MESSAGE_SIZE);
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new JocketSynchronousWriter(server);
        final ISynchronousReader<IByteBuffer> requestReader = new JocketSynchronousReader(server);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runJocketPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        TimeUnit.SECONDS.sleep(1);
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new JocketSynchronousWriter(client);
        final ISynchronousReader<IByteBuffer> responseReader = new JocketSynchronousReader(client);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    private void runJocketPerformanceTest(final JocketChannel responseAddress, final JocketChannel requestAddress)
            throws InterruptedException {
    }

}
