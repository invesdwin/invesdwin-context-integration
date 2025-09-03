package de.invesdwin.context.integration.channel.sync.kafka;

import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.NetworkSettings;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.math.decimal.scaled.ByteSizeScale;

/**
 * Hacked kafka container that advertises through the gateway so that also nifi container can talk to kafka through the
 * gateway.
 */
@NotThreadSafe
public class KafkaContainer extends org.testcontainers.kafka.KafkaContainer
        implements IKafkaContainer<org.testcontainers.kafka.KafkaContainer> {
    private String hostOverride;
    private String bootstrapServersOverride;

    public KafkaContainer() {
        this("apache/kafka:3.8.0");
    }

    public KafkaContainer(final String imageName) {
        this(DockerImageName.parse(imageName));
    }

    public KafkaContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
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

    public KafkaContainer setEnvTransient() {
        withEnv("KAFKA_LOG_RETENTION_MS", "1");
        return this;
    }

    public void setEnvTmpfs() {
        withEnv("KAFKA_LOG_DIRS", "/dev/shm/kafka-logs");
        withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
            @Override
            public void accept(final CreateContainerCmd t) {
                t.getHostConfig().withShmSize((long) ByteSizeScale.BYTES.convert(8, ByteSizeScale.GIGABYTES));
            }
        });
    }

    public static void createTopic(final String bootstrapServers, final String topic) {
        final Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(config)) {
            final NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteAllTopics(final String bootstrapServers) {
        final Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(config)) {
            final ListTopicsResult listTopics = adminClient.listTopics();
            final Set<String> names = listTopics.names().get();
            adminClient.deleteTopics(names).all().get();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}