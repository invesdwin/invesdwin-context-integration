package de.invesdwin.context.integration.channel.sync.kafka;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;

import de.invesdwin.context.integration.channel.stream.client.channel.StreamSynchronousEndpointClientReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.kafka.serde.ByteBufferProviderKafkaDeserializer;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.iterable.EmptyCloseableIterator;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class KafkaSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    public static final Duration DEFAULT_POLL_TIMEOUT = StreamSynchronousEndpointClientReader.DEFAULT_POLL_TIMEOUT;
    private final String bootstrapServersConfig;
    private final String topic;
    private final Duration pollTimeout;
    private Consumer<java.nio.ByteBuffer, IByteBufferProvider> consumer;
    private Iterator<ConsumerRecord<java.nio.ByteBuffer, IByteBufferProvider>> recordsIterator = EmptyCloseableIterator
            .getInstance();

    public KafkaSynchronousReader(final String bootstrapServersConfig, final String topic) {
        this.bootstrapServersConfig = bootstrapServersConfig;
        this.topic = topic;
        this.pollTimeout = newPollTimeout();
    }

    protected Duration newPollTimeout() {
        return DEFAULT_POLL_TIMEOUT;
    }

    @Override
    public void open() throws IOException {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "invesdwin");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferProviderKafkaDeserializer.class.getName());
        consumer = new KafkaConsumer<java.nio.ByteBuffer, IByteBufferProvider>(props);
        consumer.subscribe(Collections.singletonList(topic));//subscribes to the topic
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        final boolean hasNext = recordsIterator.hasNext();
        if (hasNext) {
            return true;
        }
        final ConsumerRecords<java.nio.ByteBuffer, IByteBufferProvider> records = consumer
                .poll(pollTimeout.javaTimeValue());
        if (records.isEmpty()) {
            recordsIterator = EmptyCloseableIterator.getInstance();
            return false;
        } else {
            recordsIterator = records.iterator();
            return true;
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final IByteBuffer message = recordsIterator.next().value().asBuffer();
        if (ClosedByteBuffer.isClosed(message)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return message;
    }

    @Override
    public void readFinished() {}

}
