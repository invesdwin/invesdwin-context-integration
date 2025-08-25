package de.invesdwin.context.integration.channel.sync.kafka.redpanda;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;

@NotThreadSafe
public class RedpandaContainer extends org.testcontainers.redpanda.RedpandaContainer
        implements IKafkaContainer<org.testcontainers.redpanda.RedpandaContainer> {
    public RedpandaContainer() {
        super(DockerImageName.parse("redpandadata/redpanda:v23.1.2"));
    }
}