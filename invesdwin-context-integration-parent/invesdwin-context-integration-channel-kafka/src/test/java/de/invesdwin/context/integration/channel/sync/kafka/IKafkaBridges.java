package de.invesdwin.context.integration.channel.sync.kafka;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;

public interface IKafkaBridges extends Startable {

    GenericContainer<?> startBridge(String inputTopic, String outputTopic);

    IKafkaBridges withNetwork(Network network);

}
