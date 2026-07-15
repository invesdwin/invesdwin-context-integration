package de.invesdwin.context.integration.channel.sync.kafka;

import java.io.IOException;
import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class KafkaSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private static final Callback ERROR_CALLBACK = new Callback() {
        @Override
        public void onCompletion(final RecordMetadata metadata, final Exception exception) {
            if (exception != null) {
                /*
                 * WARNING: some errors should be handled in a more intelligent way at some point, we want to keep
                 * message order while still being able to use full bandwidth with fire and forget async send calls in
                 * throughput scenarios
                 */
                Err.process(exception);
            }
        }
    };
    protected final String bootstrapServersConfig;
    protected final String topic;
    protected boolean flush;
    protected Producer<byte[], byte[]> producer;
    protected final byte[] key;

    public KafkaSynchronousWriter(final String bootstrapServersConfig, final String topic, final boolean flush) {
        this.bootstrapServersConfig = bootstrapServersConfig;
        this.topic = topic;
        this.flush = flush;
        this.key = newKey();
    }

    /**
     * Null keys are actually faster than giving it a key.
     */
    protected byte[] newKey() {
        return null;
    }

    @Override
    public void open() throws IOException {
        //creates the properties and initiating a producer (writer) with strings as key and M type for general values to be flexible
        final Properties props = newProducerProperties();
        producer = new KafkaProducer<byte[], byte[]>(props);
    }

    private Properties newProducerProperties() {
        final Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        //        FOR LATENCY
        //kafkaProps.put(ProducerConfig.ACKS_CONFIG, "0");
        //kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        //kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "0");
        //kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        //
        //FOR THROUGHPUT
        //kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
        //kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        //kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        return kafkaProps;
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            write(ClosedByteBuffer.INSTANCE);
            producer.flush();
            producer.close();
            producer = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return producer != null;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key,
                message.asBuffer().asByteArrayCopy());
        producer.send(record, ERROR_CALLBACK);
        if (flush) {
            producer.flush();
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
