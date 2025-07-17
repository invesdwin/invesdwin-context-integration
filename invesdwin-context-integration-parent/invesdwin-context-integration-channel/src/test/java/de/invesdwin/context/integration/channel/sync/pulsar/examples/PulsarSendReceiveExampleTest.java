package de.invesdwin.context.integration.channel.sync.pulsar.examples;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@Testcontainers
@NotThreadSafe
public class PulsarSendReceiveExampleTest extends ATest {

    private static final String TOPIC_NAME = "helloworldtopic";
    private static final String MESSAGE = "helloworldtest";
    private static final String CLIENT_URL = "pulsar://localhost:6650";
    private static final String ADMIN_URL = "http://localhost:8080";
    private static final String SUBSCRIPTION_NAME = "my subscription";
    private static final SubscriptionType SUBSCRIPTION_TYPE = SubscriptionType.Exclusive;
    private static final int NUMOFEVENTS = 10;

    @Container
    private static final PulsarContainer PULSARCONTAINER = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:3.0.0"));

    private PulsarClient newClient() {
        try {
            //Use CLIENT_URL if you dont want to use docker, otherwise use PULSARCONTAINER.getPulsarBrokerUrl()
            return PulsarClient.builder().serviceUrl(PULSARCONTAINER.getPulsarBrokerUrl()).build();
        } catch (final PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private PulsarAdmin newAdmin() {
        try {//Use ADMIN_URL if you dont want to use docker, otherwise use PULSARCONTAINER.getHttpServiceUrl()
            return PulsarAdmin.builder().serviceHttpUrl(PULSARCONTAINER.getHttpServiceUrl()).build();
        } catch (final PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private Producer<String> newProducer(final PulsarClient client) {
        log.info("producer created");
        try {
            return client.newProducer(Schema.STRING)
                    //                    .accessMode(ProducerAccessMode.Exclusive)
                    .topic(TOPIC_NAME)
                    .create();
        } catch (final PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private Consumer<String> newConsumer(final PulsarClient client) {
        try {
            return client.newConsumer(Schema.STRING)
                    .topic(TOPIC_NAME)
                    .subscriptionName(UUIDs.newPseudoRandomUUID())
                    .subscriptionType(SUBSCRIPTION_TYPE)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
        } catch (final PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private Reader<String> newReader(final PulsarClient client) {
        try {
            return client.newReader(Schema.STRING)
                    .topic(TOPIC_NAME)
                    .subscriptionName(UUIDs.newPseudoRandomUUID())
                    .startMessageId(MessageId.earliest)
                    .create();
        } catch (final PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test() throws Exception {
        //creating the admin to create and delete topics

        try (PulsarAdmin admin = newAdmin()) {
            final List<String> tenants = admin.tenants().getTenants();
            for (final String tenant : tenants) {
                log.info(tenant);
                final List<String> namespaces = admin.namespaces().getNamespaces(tenant);
                for (final String namespace : namespaces) {
                    if ("public/functions".equals(namespace)) {
                        continue;
                    }
                    final List<String> topics = admin.topics().getList(namespace);
                    for (final String topic : topics) {
                        log.info("deleting topic [%s] [%s] [%s]", tenant, namespace, topic);
                        admin.topics().unload(topic);
                        admin.topics().delete(topic);
                    }
                }
            }
        }
        //final PulsarAdmin admin = newAdmin();
        //try {
        //  admin.topics().createNonPartitionedTopic(TOPIC_NAME);
        //} catch (final PulsarAdminException e) {
        //  throw new RuntimeException(e);
        //}

        final Instant start = new Instant();
        //log.info("topic created %s", TOPIC_NAME);
        //creating the client to handle the communication between producer and consumer
        log.info("client created");
        //creates and starts a thread, consumer, and runs the consume method
        final Thread producer = new Thread("producer") {
            @Override
            public void run() {
                produce();
            }
        };
        producer.start();

        //creates and starts a thread, consumer, and runs the consume method
        final Thread consumer = new Thread("consumer") {
            @Override
            public void run() {
                //consume();
                read();
            }
        };
        consumer.start();
        while (consumer.isAlive() || producer.isAlive()) {
            FTimeUnit.MILLISECONDS.sleepNoInterrupt(1);
        }
        final Duration duration = start.toDuration();
        log.info("Finished %s messages with %s after %s", NUMOFEVENTS,
                new ProcessedEventsRateString(NUMOFEVENTS, duration), duration);
    }

    private void produce() {
        final PulsarClient client = newClient();
        //creating producer and sending a message
        final Producer<String> producer = newProducer(client);
        try {
            for (int i = 0; i < NUMOFEVENTS; i++) {
                final MessageId sentMessageId = producer.newMessage().value(MESSAGE + i).send();
                log.info("Producer sent message %s %s", sentMessageId, MESSAGE + i);
            }

            producer.close();
        } catch (final PulsarClientException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(producer);
            Closeables.closeQuietly(client);
        }
        log.info("producer finished");
    }

    private void consume() {

        final PulsarClient client = newClient();
        final Consumer<String> consumer = newConsumer(client);
        try {
            int i = 0;
            while (i < NUMOFEVENTS) {
                // Listening and waiting for a message
                final Message<String> receivedMessage = consumer.receive();
                // Print out that the message has been received

                log.info("Consumer received message: %s %s", receivedMessage.getMessageId(),
                        receivedMessage.getValue());
                i++;
                // Send an acknowledgment back so that it can be deleted by the broker
                consumer.acknowledge(receivedMessage);

            }

        } catch (final PulsarClientException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(consumer);
            Closeables.closeQuietly(client);
        }
    }

    private void read() {
        final PulsarClient client = newClient();
        final Reader<String> reader = newReader(client);
        try {
            int i = 0;
            while (i < NUMOFEVENTS) {
                // Listening and waiting for a message
                final Message<String> receivedMessage = reader.readNext();
                // Print out that the message has been received
                i++;
                log.info("Reader received message: %s %s", receivedMessage.getMessageId(), receivedMessage.getValue());
                // Send an acknowledgment back so that it can be deleted by the broker
            }

        } catch (final PulsarClientException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(reader);
            Closeables.closeQuietly(client);
        }
    }

}