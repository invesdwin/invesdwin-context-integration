package de.invesdwin.context.integration.channel.sync.kafka;

import java.io.IOException;
import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class KafkaSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    protected final String bootstratServersConfig;
    protected final String topic;
    protected Producer<byte[], byte[]> producer;
    protected final byte[] key;

    //constructor with serverconfig, topic to send messages to and key
    public KafkaSynchronousWriter(final String bootstratServersConfig, final String topic) {
        this.bootstratServersConfig = bootstratServersConfig;
        this.topic = topic;
        this.key = newKey().getBytes();
    }

    //method to create a random key id
    protected String newKey() {
        return UUIDs.newPseudoRandomUUID();
    }

    @Override
    public void open() throws IOException {
        //creates the properties and initiating a producer (writer) with strings as key and M type for general values to be flexible
        final Properties props = newProducerProperties();
        producer = new KafkaProducer<byte[], byte[]>(props);
    }

    private Properties newProducerProperties() {
        final Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstratServersConfig);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
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
        //checks if writer was made and isnt null
        return producer != null;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key,
                message.asBuffer().asByteArrayCopy());
        producer.send(record);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
