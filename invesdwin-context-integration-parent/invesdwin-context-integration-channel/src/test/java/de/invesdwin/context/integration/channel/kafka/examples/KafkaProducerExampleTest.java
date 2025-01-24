// CHECKSTYLE:OFF
package de.invesdwin.context.integration.channel.kafka.examples;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
// till here

// added from the example of the hello world
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka console message producer. Takes in text (ending with newline) and sends it to a Kafka topic.
 */
public class KafkaProducerExampleTest {
    public final static String TOPIC_NAME = "testtopic";
    public final static Logger logger = LoggerFactory.getLogger(KafkaProducerExampleTest.class.getName());

    public static void main(final String[] args) throws InterruptedException, ExecutionException {
        // produce a test message
        // if u run this multiple times ... u will have multiple messages in the
        // test_topic topic (as would be expected)
        final Producer<String, String> producer = KafkaProducerExampleTest.createProducer();

        final Scanner sc = new Scanner(System.in);
        try {
            String inputText = null;
            while (!"exit".equalsIgnoreCase(inputText)) {
                inputText = sc.nextLine();

                // key is hardcoded to 'key1', which forces all messages to go to a single partition as per kafka behavior
                final ProducerRecord<String, String> recordToSend = new ProducerRecord<>(TOPIC_NAME, "key1", inputText);

                // asynchronous send
                producer.send(recordToSend, (recordMetadata, e) -> {
                    if (e == null) {
                        logger.info("Message Sent. topic={}, partition={}, offset={}", recordMetadata.topic(),
                                recordMetadata.partition(), recordMetadata.offset());
                    } else {
                        logger.error("Error while sending message. ", e);
                    }
                });

            }
        } finally {
            sc.close();
            producer.flush();
            producer.close();
        }
    }

    private static Producer<String, String> createProducer() {
        final Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(kafkaProps);
    }

}