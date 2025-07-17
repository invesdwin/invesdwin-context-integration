package de.invesdwin.context.integration.channel.sync.pulsar;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.schema.BytesSchema;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class PulsarProducerSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private static final Function<Throwable, MessageId> ERROR_HANDLER = new Function<Throwable, MessageId>() {
        @Override
        public MessageId apply(final Throwable error) {
            if (error != null) {
                /*
                 * WARNING: at least ProducerQueueIsFullError should be handled in a more intelligent way at some point,
                 * we don't want to use blockIfQueueFull(true) here in a non-blocking writer and we want to keep message
                 * order while still being able to use full bandwidth with fire and forget async send calls in
                 * throughput scenarios
                 */
                Err.process(error);
            }
            return null;
        }
    };
    protected final PulsarSynchronousChannel channel;
    protected final String topic;
    protected final boolean flush;
    protected final boolean blocking;
    protected Producer<byte[]> producer;

    public PulsarProducerSynchronousWriter(final PulsarSynchronousChannel channel, final String topic,
            final boolean flush, final boolean blocking) {
        this.channel = channel;
        this.topic = topic;
        this.flush = flush;
        this.blocking = blocking;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        producer = newProducer(channel.getPulsarClient());
    }

    protected Producer<byte[]> newProducer(final PulsarClient client) throws PulsarClientException {
        return newProducerBuilder(client).create();
    }

    protected ProducerBuilder<byte[]> newProducerBuilder(final PulsarClient client) {
        return client.newProducer(BytesSchema.of())
                .topic(topic)
                .producerName(newProducerName())
                .enableBatching(!flush)
                .blockIfQueueFull(blocking)
                .sendTimeout(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    protected String newProducerName() {
        return UUIDs.newPseudoRandomUUID();
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            write(ClosedByteBuffer.INSTANCE);
            producer.flush();
            producer.close();
            producer = null;
            channel.close();
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        //checks if writer was made and isnt null
        return producer != null;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final TypedMessageBuilder<byte[]> msg = producer.newMessage().value(message.asBuffer().asByteArrayCopy());
        if (blocking && flush) {
            msg.send();
            producer.flush();
        } else {
            msg.sendAsync().exceptionally(ERROR_HANDLER);
            if (flush) {
                producer.flushAsync();
            }
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}