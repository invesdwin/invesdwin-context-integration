package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FTimeUnit;

@Disabled("sometimes deadlocks due to blocking nature of the socket")
@NotThreadSafe
public class BidiBlockingDatagramChannelTest extends AChannelTest {

    @Test
    public void testBlockingDatagramPerformance() throws InterruptedException {
        testBlockingDatagramPerformanceTry(0);
    }

    private void testBlockingDatagramPerformanceTry(final int tries) throws InterruptedException {
        final int port = NetworkUtil.findAvailableUdpPort() + tries;
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        try {
            runBlockingDatagramPerformanceTest(address);
        } catch (final Throwable t) {
            //workaround needed for testsuite because ports kind of stay blocked sometimes
            if (Throwables.isCausedByType(t, PortUnreachableException.class) && tries < 100) {
                Err.process(new Exception("ignoring and retrying", t));
                FTimeUnit.MILLISECONDS.sleep(10);
                testBlockingDatagramPerformanceTry(tries + 1);
            } else {
                throw t;
            }
        }
    }

    protected void runBlockingDatagramPerformanceTest(final SocketAddress address) throws InterruptedException {
        final boolean lowLatency = true;
        final BlockingDatagramSynchronousChannel serverChannel = newBlockingDatagramSynchronousChannel(address, true,
                getMaxMessageSize(), lowLatency);
        final BlockingDatagramSynchronousChannel clientChannel = newBlockingDatagramSynchronousChannel(address, false,
                getMaxMessageSize(), lowLatency);

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new BlockingDatagramSynchronousWriter(
                serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new BlockingDatagramSynchronousReader(
                serverChannel);
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new BlockingDatagramSynchronousWriter(
                clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new BlockingDatagramSynchronousReader(
                clientChannel);
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected BlockingDatagramSynchronousChannel newBlockingDatagramSynchronousChannel(
            final SocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize,
            final boolean lowLatency) {
        return new BlockingDatagramSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

}
