package de.invesdwin.context.integration.channel.sync.kafka.redpanda;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.NetworkSettings;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.string.Strings;

@NotThreadSafe
public class RedpandaContainer extends org.testcontainers.redpanda.RedpandaContainer
        implements IKafkaContainer<org.testcontainers.redpanda.RedpandaContainer> {
    private String hostOverride;

    public RedpandaContainer() {
        super(DockerImageName.parse("redpandadata/redpanda:v25.2.2"));
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void containerIsStarting(final InspectContainerResponse containerInfo) {
        final NetworkSettings networkSettings = containerInfo.getNetworkSettings();
        this.hostOverride = networkSettings.getGateway();
        super.containerIsStarting(containerInfo);
    }

    @Override
    public String getBootstrapServers() {
        final String b = super.getBootstrapServers();
        return Strings.removeStart(b, "PLAINTEXT://");
    }

    @Override
    public String getHost() {
        Assertions.checkNotNull(hostOverride);
        return hostOverride;
    }
}