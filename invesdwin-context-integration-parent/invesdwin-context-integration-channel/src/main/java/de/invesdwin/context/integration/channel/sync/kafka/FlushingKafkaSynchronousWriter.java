package de.invesdwin.context.integration.channel.sync.kafka;

import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import de.invesdwin.util.concurrent.future.NullFuture;

/**
 * Flushes messages immediately to reduce latency for individual messages.
 */
@NotThreadSafe
public class FlushingKafkaSynchronousWriter<M> extends KafkaSynchronousWriter<M> {

    private Future<RecordMetadata> writeFlushed = NullFuture.getInstance();

    public FlushingKafkaSynchronousWriter(final String bootstratServersConfig, final String topic) {
        super(bootstratServersConfig, topic);
    }

    @Override
    public void write(final M message) throws IOException {
        final ProducerRecord<byte[], M> record = new ProducerRecord<>(topic, key, message);
        writeFlushed = producer.send(record);
        producer.flush();
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return writeFlushed.isDone();
    }

}
