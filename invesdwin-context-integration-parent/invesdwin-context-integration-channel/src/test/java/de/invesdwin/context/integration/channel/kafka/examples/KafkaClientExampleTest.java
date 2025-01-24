// CHECKSTYLE:OFF
package de.invesdwin.context.integration.channel.kafka.examples;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka console message consumer.
 */
public class KafkaClientExampleTest {
    private final static String TOPIC_NAME = KafkaProducerExampleTest.TOPIC_NAME;
    public final static Logger logger = LoggerFactory.getLogger(KafkaClientExampleTest.class.getName());

    public static void main(final String[] args) {
        // consume messages
        final Consumer<String, String> consumer = KafkaClientExampleTest.createConsumer();

        // subscribe to the test topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        try {
            String receivedText = null;
            while (!"exit".equalsIgnoreCase(receivedText)) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (final ConsumerRecord<String, String> record : records) {
                    receivedText = record.value();
                    if (receivedText != null) {
                        logger.info(
                                "Message received ==> topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                record.topic(), record.partition(), record.offset(), record.key(), receivedText);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static Consumer<String, String> createConsumer() {
        final Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer_group");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<String, String>(kafkaProps);
    }
}