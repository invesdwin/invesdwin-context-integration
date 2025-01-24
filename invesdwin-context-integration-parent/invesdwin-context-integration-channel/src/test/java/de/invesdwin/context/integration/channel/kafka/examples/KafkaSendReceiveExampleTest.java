package de.invesdwin.context.integration.channel.kafka.examples;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
@Testcontainers
public class KafkaSendReceiveExampleTest extends ATest {

    private static final String TOPIC_NAME = "helloworldtopic1";// name of kafka topic
    private static final String MESSAGE = "helloworldtest"; //message to be sent
    private static final String KEY = "key1"; //partition key, messages with the same key go to the same partition
    private static final int NUMOFEVENTS = 1000;

    //using apache kafka's image 3.8.0 where testContainers will automatically start and stop containers
    @Container
    private static final KafkaContainer KAFKACONTAINER = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.8.0"));

    //volatile variable meaning its visibility is available across threads
    private volatile boolean consumerSubscribed = false; //flag to signal the consumer has subscribed

    @Test //Clarifying its a test
    public void test() throws InterruptedException, ExecutionException {

        //Creating a topic using AdminClient
        final Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKACONTAINER.getBootstrapServers());

        final Instant start = new Instant();
        try (AdminClient adminClient = AdminClient.create(config)) {

            /*
             * this code was used to identify if the topic exists, but there isn't a topic inside unless we define it
             * before the test
             * 
             * // The topic to check final String topicName = TOPIC_NAME;
             *
             * 
             * // Listing currently existing topics final ListTopicsResult topicsResult = adminClient.listTopics();
             * final Set<String> existingTopics = topicsResult.names().get();
             * 
             * // Checking if the topic exists if (existingTopics.contains(topicName)) { log.info("Topic '" + topicName
             * + "' found. Deleting now...");
             * 
             * // deleting the topic final DeleteTopicsResult deleteTopicsResult = adminClient
             * .deleteTopics(Collections.singletonList(topicName));
             * 
             * // waiting for the delete operation to complete deleteTopicsResult.all().get(); log.info("Topic '" +
             * topicName + "' deleted successfully!"); } else { log.info("Topic '" + topicName +
             * "' does not exist. Nothing to delete."); }
             */
            // Defining the new topic with a name, partition and replication factor
            final NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);

            // Creating the topic
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        }
        log.info("topic created after %s", start);
        //creates and starts a thread, consumer, and runs the consume method
        final Thread consumer = new Thread() {
            @Override //overriding the run method
            public void run() {
                consume();
            }
        };
        consumer.start();

        final Thread producer = new Thread() {
            @Override
            public void run() {
                produce();
            }
        };
        producer.start();
        while (consumer.isAlive() || producer.isAlive()) {
            FTimeUnit.MILLISECONDS.sleepNoInterrupt(1);
        }

        final Duration duration = start.toDuration();
        log.info("Finished %s messages with %s after %s", NUMOFEVENTS,
                new ProcessedEventsRateString(NUMOFEVENTS, duration), duration);

    }

    private void produce() {
        final Instant startOverall = new Instant();
        //constantly checks if the consumer has subscribed before sending a message
        while (!consumerSubscribed) {
            FTimeUnit.MILLISECONDS.sleepNoInterrupt(1);
        }
        final Producer<String, String> producer = createProducer();// creates a producer using kafka's methods
        final Instant startMessaging = new Instant();
        try {
            for (int i = 0; i < NUMOFEVENTS; i++) {
                final ProducerRecord<String, String> recordToSend = new ProducerRecord<>(TOPIC_NAME, KEY, MESSAGE + i);
                producer.send(recordToSend, (recordMetadata, e) -> {
                    // code for logging the messages
                    /*
                     * if (e == null) { log.info("Message Sent. topic=%s, partition=%s, offset=%s",
                     * recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), message); } else {
                     * log.error("Error while sending message. ", e); }
                     */

                });
            }
        } finally {
            producer.flush();
            producer.close();
            final Duration durationOverall = startOverall.toDuration();
            final Duration durationMessaging = startMessaging.toDuration();
            log.info("producer finished after %s|%s with %s|%s messages", durationOverall, durationMessaging,
                    new ProcessedEventsRateString(NUMOFEVENTS, durationOverall),
                    new ProcessedEventsRateString(NUMOFEVENTS, durationMessaging));
        }
    }

    private void consume() {
        final Instant startOverall = new Instant();
        final Consumer<String, String> consumer = createConsumer();//consumer created using kafka's methods

        // subscribe to the topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        consumerSubscribed = true;

        Instant startMessaging = new Instant();
        final LoopInterruptedCheck loopCheck = new LoopInterruptedCheck(Duration.ONE_SECOND);

        try {
            int i = 0;
            String receivedText = null;
            ConsumerRecords<String, String> records = null;
            //Continuously polls to check for messages to retrieve from the broker until NUMOFEVENTS messages have been pooled
            while (i < NUMOFEVENTS - 1) {
                records = consumer.poll(new Duration(100, FTimeUnit.MILLISECONDS).javaTimeValue());

                for (final ConsumerRecord<String, String> record : records) {
                    receivedText = record.value();
                    if (startMessaging == null) {
                        startMessaging = new Instant();
                    }
                    Assertions.checkEquals(MESSAGE + i, receivedText);
                    //log.info("Message received ==> topic = %s, partition = %s, offset = %s, key = %s, value = %s",
                    //      record.topic(), record.partition(), record.offset(), record.key(), receivedText);
                    i++;
                    if (loopCheck.checkClockNoInterrupt()) {
                        final Duration durationMessaging = startMessaging.toDuration();
                        log.info("consumer received %s messages with %s since %s", i,
                                new ProcessedEventsRateString(NUMOFEVENTS, durationMessaging), durationMessaging);
                    }
                }

            }
        } finally {
            consumer.close();
            final Duration durationOverall = startOverall.toDuration();
            final Duration durationMessaging = startMessaging.toDuration();
            log.info("consumer finished after %s|%s with %s|%s messages", durationOverall, durationMessaging,
                    new ProcessedEventsRateString(NUMOFEVENTS, durationOverall),
                    new ProcessedEventsRateString(NUMOFEVENTS, durationMessaging));
        }
    }

    private static Producer<String, String> createProducer() {
        final Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKACONTAINER.getBootstrapServers());
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(kafkaProps);
    }

    private static Consumer<String, String> createConsumer() {
        final Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKACONTAINER.getBootstrapServers());
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer_group");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<String, String>(kafkaProps);
    }

}