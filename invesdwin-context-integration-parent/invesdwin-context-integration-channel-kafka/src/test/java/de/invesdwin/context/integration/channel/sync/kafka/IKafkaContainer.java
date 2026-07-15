package de.invesdwin.context.integration.channel.sync.kafka;

import org.testcontainers.containers.Container;
import org.testcontainers.lifecycle.Startable;

public interface IKafkaContainer<SELF extends Container<SELF>> extends Startable {

    String getBootstrapServers();

}
