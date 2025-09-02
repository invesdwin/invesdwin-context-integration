package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.kafka.ConfluentKafkaContainer;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.NetworkSettings;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.util.assertions.Assertions;

@SuppressWarnings("deprecation")
@NotThreadSafe
public class ConfluentServerContainer extends net.christophschubert.cp.testcontainers.ConfluentServerContainer
        implements IKafkaContainer<ConfluentKafkaContainer> {

    public static final String REPOSITORY = "confluentinc";
    /**
     * confluent 8.0.0 does not start with cp-testcontainers yet, maybe the project needs to be updated to make it work
     */
    public static final String TAG = "7.8.3";
    private String hostOverride;

    public ConfluentServerContainer() {
        super(REPOSITORY, TAG);
    }

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
