package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.KafkaContainer;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;

@SuppressWarnings("deprecation")
@NotThreadSafe
public class ConfluentServerContainer extends net.christophschubert.cp.testcontainers.ConfluentServerContainer
        implements IKafkaContainer<KafkaContainer> {

    public static final String REPOSITORY = "confluentinc";
    /**
     * confluent 8.0.0 does not start with cp-testcontainers yet, maybe the project needs to be updated to make it work
     */
    public static final String TAG = "7.8.3";

    public ConfluentServerContainer() {
        super(REPOSITORY, TAG);
    }
}
