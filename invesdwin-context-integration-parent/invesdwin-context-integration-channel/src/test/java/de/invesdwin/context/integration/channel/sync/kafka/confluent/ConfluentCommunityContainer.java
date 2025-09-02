package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.NetworkSettings;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class ConfluentCommunityContainer extends org.testcontainers.kafka.ConfluentKafkaContainer
        implements IKafkaContainer<org.testcontainers.kafka.KafkaContainer> {
    private String hostOverride;

    public ConfluentCommunityContainer() {
        super(DockerImageName.parse("confluentinc/cp-kafka:8.0.0"));
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void containerIsStarting(final InspectContainerResponse containerInfo) {
        final NetworkSettings networkSettings = containerInfo.getNetworkSettings();
        this.hostOverride = networkSettings.getGateway();
        super.containerIsStarting(containerInfo);
    }

    @Override
    public String getHost() {
        Assertions.checkNotNull(hostOverride);
        return hostOverride;
    }
}