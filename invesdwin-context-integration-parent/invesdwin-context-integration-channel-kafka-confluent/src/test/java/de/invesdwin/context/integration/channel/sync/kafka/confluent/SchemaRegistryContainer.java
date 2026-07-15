package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.NetworkSettings;

import de.invesdwin.context.integration.channel.sync.kafka.ISchemaRegistryContainer;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class SchemaRegistryContainer extends net.christophschubert.cp.testcontainers.SchemaRegistryContainerAccessor
        implements ISchemaRegistryContainer<net.christophschubert.cp.testcontainers.SchemaRegistryContainer> {

    private String hostOverride;

    public SchemaRegistryContainer(final ConfluentServerContainer bootstrap, final Network network) {
        super(DockerImageName
                .parse(ConfluentServerContainer.REPOSITORY + "/cp-schema-registry:" + ConfluentServerContainer.TAG),
                bootstrap, network);
    }

    @Override
    protected void configure() {
        super.configure();
        withProperty("kafkastore.bootstrap.servers", getBootstrap().getBootstrapServers());
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void containerIsStarting(final InspectContainerResponse containerInfo) {
        final NetworkSettings networkSettings = containerInfo.getNetworkSettings();
        //relay communication over port forwarding from host computer
        this.hostOverride = networkSettings.getGateway();
        super.containerIsStarting(containerInfo);
    }

    @Override
    public String getHost() {
        Assertions.checkNotNull(hostOverride);
        return hostOverride;
    }

    @Override
    public String getSchemaRegistryUrl() {
        return getBaseUrl();
    }
}
