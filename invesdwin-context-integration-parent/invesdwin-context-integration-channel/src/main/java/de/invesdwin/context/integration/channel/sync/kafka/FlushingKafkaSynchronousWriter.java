package de.invesdwin.context.integration.channel.sync.kafka;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.producer.ProducerRecord;

import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Flushes messages immediately to reduce latency for individual messages.
 */
@NotThreadSafe
public class FlushingKafkaSynchronousWriter extends KafkaSynchronousWriter {

    public FlushingKafkaSynchronousWriter(final String bootstrapServersConfig, final String topic) {
        super(bootstrapServersConfig, topic);
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key,
                message.asBuffer().asByteArray());
        producer.send(record);
        producer.flush();
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
