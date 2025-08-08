package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;

@NotThreadSafe
public class ConfluentCommunityContainer extends org.testcontainers.kafka.ConfluentKafkaContainer
        implements IKafkaContainer<org.testcontainers.kafka.KafkaContainer> {
    public ConfluentCommunityContainer() {
        super(DockerImageName.parse("confluentinc/cp-kafka:8.0.0"));

    }
}