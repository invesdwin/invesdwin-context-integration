package de.invesdwin.context.integration.channel.sync.kafka;

import org.testcontainers.containers.Container;
import org.testcontainers.lifecycle.Startable;

public interface ISchemaRegistryContainer<SELF extends Container<SELF>> extends Startable {

    String getSchemaRegistryUrl();

}
