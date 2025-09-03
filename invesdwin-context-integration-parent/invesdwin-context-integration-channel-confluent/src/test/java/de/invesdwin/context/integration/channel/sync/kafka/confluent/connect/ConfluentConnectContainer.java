package de.invesdwin.context.integration.channel.sync.kafka.confluent.connect;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.NetworkSettings;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaConnectContainer;
import de.invesdwin.util.assertions.Assertions;
import net.christophschubert.cp.testcontainers.KafkaConnectContainerAccessor;

@NotThreadSafe
public class ConfluentConnectContainer extends KafkaConnectContainerAccessor
        implements IKafkaConnectContainer<net.christophschubert.cp.testcontainers.KafkaConnectContainer> {
    private String hostOverride;

    public ConfluentConnectContainer(final KafkaContainer bootstrap, final Network network) {
        super(DockerImageName.parse("confluentinc/cp-kafka-connect:7.2.15"), bootstrap, network);
        dependsOn(bootstrap);
    }

    @Override
    protected void configure() {
        super.configure();
        withProperty("bootstrap.servers", getBootstrap().getBootstrapServers());
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
    public String getKafkaConnectUrl() {
        return getBaseUrl();
    }

}