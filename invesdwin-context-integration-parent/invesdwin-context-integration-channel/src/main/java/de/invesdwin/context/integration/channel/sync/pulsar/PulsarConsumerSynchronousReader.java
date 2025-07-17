package de.invesdwin.context.integration.channel.sync.pulsar;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.pulsar.serde.ByteBufferProviderPulsarSchema;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class PulsarConsumerSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    public static final Duration DEFAULT_POLL_TIMEOUT = StreamSynchronousEndpointClientReader.DEFAULT_POLL_TIMEOUT;

    protected final PulsarSynchronousChannel channel;
    protected final String topic;
    protected final Duration pollTimeout;
    protected Consumer<IByteBufferProvider> consumer;
    private Message<IByteBufferProvider> message;

    public PulsarConsumerSynchronousReader(final PulsarSynchronousChannel channel, final String topic) {
        this.channel = channel;
        this.topic = topic;
        this.pollTimeout = newPollTimeout();
    }

    protected Duration newPollTimeout() {
        return DEFAULT_POLL_TIMEOUT;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        consumer = newConsumer(channel.getPulsarClient());
    }

    protected Consumer<IByteBufferProvider> newConsumer(final PulsarClient client) throws PulsarClientException {
        return newConsumerBuilder(client).subscribe();
    }

    protected ConsumerBuilder<IByteBufferProvider> newConsumerBuilder(final PulsarClient client) {
        return client.newConsumer(ByteBufferProviderPulsarSchema.of())
                .topic(topic)
                .consumerName(newConsumerName())
                .subscriptionName(UUIDs.newPseudoRandomUUID())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .poolMessages(true);
    }

    protected String newConsumerName() {
        return UUIDs.newPseudoRandomUUID();
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        channel.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (message != null) {
            return true;
        }
        message = consumer.receive(pollTimeout.intValue(), pollTimeout.getTimeUnit().timeUnitValue());
        return message != null;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        consumer.acknowledgeCumulativeAsync(message);

        final IByteBuffer msg = message.getValue().asBuffer();
        if (ClosedByteBuffer.isClosed(msg)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return msg;
    }

    @Override
    public void readFinished() throws IOException {
        message.release();
        message = null;
    }

}