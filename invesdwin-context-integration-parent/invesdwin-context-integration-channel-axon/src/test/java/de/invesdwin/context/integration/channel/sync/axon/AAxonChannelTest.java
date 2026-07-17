package de.invesdwin.context.integration.channel.sync.axon;

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
import de.invesdwin.context.integration.channel.sync.axon.channel.AAxonSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class AAxonChannelTest extends AChannelTest {

    private static final Duration POLL_TIMEOUT = Duration.ZERO;

    @Test
    public void testAxonLatency() throws InterruptedException {
        final String responseTopic = "testAxonLatency_response";
        final String requestTopic = "testAxonLatency_request";
        runAxonLatencyTest(responseTopic, requestTopic, true);
    }

    @Test
    public void testAxonThroughput() throws InterruptedException {
        final String topic = "testAxonThroughput_channel";
        runAxonThroughputTest(topic);
    }

    protected void runAxonThroughputTest(final String topic) throws InterruptedException {
        final ISynchronousWriter<FDate> channelWriter = newSerdeWriter(
                newAxonSynchronousWriter(getAxonSynchronousChannel(false, topic), topic));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(this, channelWriter);
        final ISynchronousReader<FDate> channelReader = newSerdeReader(
                newAxonSynchronousReader(getAxonSynchronousChannel(true, topic), topic));
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, channelReader);
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
    }

    protected void runAxonLatencyTest(final String responseTopic, final String requestTopic, final boolean useReader)
            throws InterruptedException {
        final ISynchronousReader<FDate> requestReader = newSerdeReader(
                newAxonSynchronousReader(getAxonSynchronousChannel(true, requestTopic), requestTopic));
        final ISynchronousWriter<FDate> responseWriter = newSerdeWriter(
                newAxonSynchronousWriter(getAxonSynchronousChannel(true, responseTopic), responseTopic));
        final LatencyServerTask serverTask = new LatencyServerTask(this, requestReader, responseWriter);

        final ISynchronousWriter<FDate> requestWriter = newSerdeWriter(
                newAxonSynchronousWriter(getAxonSynchronousChannel(false, requestTopic), requestTopic));
        final ISynchronousReader<FDate> responseReader = newSerdeReader(
                newAxonSynchronousReader(getAxonSynchronousChannel(false, responseTopic), responseTopic));
        final LatencyClientTask clientTask = new LatencyClientTask(this, requestWriter, responseReader);

        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected abstract AAxonSynchronousChannel getAxonSynchronousChannel(boolean server, String topic);

    protected ISynchronousReader<IByteBufferProvider> newAxonSynchronousReader(final AAxonSynchronousChannel channel,
            final String topic) {
        return new AxonSynchronousReader(channel, topic) {
            @Override
            protected Duration newPollTimeout() {
                return AAxonChannelTest.this.newPollTimeout();
            }
        };
    }

    protected Duration newPollTimeout() {
        return POLL_TIMEOUT;
    }

    protected ISynchronousWriter<IByteBufferProvider> newAxonSynchronousWriter(final AAxonSynchronousChannel channel,
            final String topic) {
        return new AxonSynchronousWriter(channel, topic);
    }
}
