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
    public static final String TAG = "v25.2.2";
    private String hostOverride;
    private String bootstrapServersOverride;

    public RedpandaContainer() {
        super(DockerImageName.parse("redpandadata/redpanda:" + TAG));
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void containerIsStarting(final InspectContainerResponse containerInfo) {
        final NetworkSettings networkSettings = containerInfo.getNetworkSettings();
        //relay communication over port forwarding from host computer
        this.hostOverride = networkSettings.getGateway();
        super.containerIsStarting(containerInfo);
        /*
         * other services connect without a protocol definition, though inside containerisStarting the protocol
         * definition is needed for advertisedListeners
         */
        this.bootstrapServersOverride = Strings.removeStart(super.getBootstrapServers(), "PLAINTEXT://");
    }

    @Override
    public String getHost() {
        Assertions.checkNotNull(hostOverride);
        return hostOverride;
    }

    @Override
    public String getBootstrapServers() {
        if (bootstrapServersOverride != null) {
            return bootstrapServersOverride;
        } else {
            return super.getBootstrapServers();
        }
    }
}