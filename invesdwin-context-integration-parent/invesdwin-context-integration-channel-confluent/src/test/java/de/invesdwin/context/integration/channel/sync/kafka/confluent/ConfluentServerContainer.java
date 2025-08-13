package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.KafkaContainer;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;

@SuppressWarnings("deprecation")
@NotThreadSafe
public class ConfluentServerContainer extends net.christophschubert.cp.testcontainers.ConfluentServerContainer
        implements IKafkaContainer<KafkaContainer> {

    public static final String REPOSITORY = "confluentinc";
    public static final String TAG = "7.8.3";

    public ConfluentServerContainer() {
        super(REPOSITORY, TAG);
    }
}
