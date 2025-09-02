package de.invesdwin.context.integration.channel.sync.kafka.confluent.connect;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import net.christophschubert.cp.testcontainers.KafkaConnectContainerAccessor;

@NotThreadSafe
public class ConfluentBridgesContainer extends KafkaConnectContainerAccessor {
    public ConfluentBridgesContainer(final KafkaContainer bootstrap, final Network network) {
        super(DockerImageName.parse("confluentinc/cp-kafka-connect:7.2.15"), bootstrap, network);

    }

}