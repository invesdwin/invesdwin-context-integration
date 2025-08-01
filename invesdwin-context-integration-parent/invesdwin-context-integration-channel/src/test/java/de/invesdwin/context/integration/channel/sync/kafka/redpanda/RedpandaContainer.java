package de.invesdwin.context.integration.channel.sync.kafka.redpanda;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;

@NotThreadSafe
public class RedpandaContainer extends org.testcontainers.redpanda.RedpandaContainer
        implements IKafkaContainer<org.testcontainers.redpanda.RedpandaContainer> {
    public RedpandaContainer() {
        super(DockerImageName.parse("redpandadata/redpanda:v23.1.2"));
        //        withEnv("REDPANDA_EXTRA_ARGS", "--overprovisioned --smp 1 --reserve-memory 0M");
        //        withEnv("REDPANDA_LOG_SEGMENT_SIZE", "134217728"); // 128MB segments
        //        withEnv("REDPANDA_DISABLE_PUBLIC_METRICS", "true");
        //        withEnv("REDPANDA_EXTRA_ARGS", "--mode dev-container");
        //        withEnv("REDPANDA_EMPTY_SEED_STARTS_CLUSTER", "false");

    }
}