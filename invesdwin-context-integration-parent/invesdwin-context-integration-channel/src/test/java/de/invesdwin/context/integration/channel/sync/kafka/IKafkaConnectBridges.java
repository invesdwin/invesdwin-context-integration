package de.invesdwin.context.integration.channel.sync.kafka;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startable;

public interface IKafkaConnectBridges extends Startable {

    GenericContainer<?> startBridge(String inputTopic, String outputTopic);

}
