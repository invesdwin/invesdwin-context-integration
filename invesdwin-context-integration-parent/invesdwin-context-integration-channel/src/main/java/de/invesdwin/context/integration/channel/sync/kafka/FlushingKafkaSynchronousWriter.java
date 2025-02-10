package de.invesdwin.context.integration.channel.sync.kafka;

import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import de.invesdwin.util.concurrent.future.NullFuture;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Flushes messages immediately to reduce latency for individual messages.
 */
@NotThreadSafe
public class FlushingKafkaSynchronousWriter extends KafkaSynchronousWriter {

    private Future<RecordMetadata> writeFlushed = NullFuture.getInstance();

    public FlushingKafkaSynchronousWriter(final String bootstratServersConfig, final String topic) {
        super(bootstratServersConfig, topic);
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key,
                message.asBuffer().asByteArray());
        writeFlushed = producer.send(record);
        producer.flush();
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return writeFlushed.isDone();
    }

}
