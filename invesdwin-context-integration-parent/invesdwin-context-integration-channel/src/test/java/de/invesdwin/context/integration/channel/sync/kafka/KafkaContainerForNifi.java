package de.invesdwin.context.integration.channel.sync.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.kafka.KafkaHelperAccessor;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.NetworkSettings;

import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.math.decimal.scaled.ByteSizeScale;

/**
 * Hacked kafka container that advertises through the gateway so that also nifi container can talk to kafka through the
 * gateway.
 */
@NotThreadSafe
public class KafkaContainerForNifi extends KafkaContainer {
    private String bootstrapServersOverride;

    public KafkaContainerForNifi() {
        this("apache/kafka:3.8.0");
    }

    public KafkaContainerForNifi(final String imageName) {
        this(DockerImageName.parse(imageName));
    }

    public KafkaContainerForNifi(final DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    protected void containerIsStarting(final InspectContainerResponse containerInfo) {
        final NetworkSettings networkSettings = containerInfo.getNetworkSettings();
        final String containerIp = networkSettings.getIpAddress();
        final String gateway = networkSettings.getGateway();

        //CHECKSTYLE:OFF
        final String brokerAdvertisedListener = String.format("BROKER://%s:%s", containerIp, "9093");
        //CHECKSTYLE:ON
        final List<String> advertisedListeners = new ArrayList<>();
        this.bootstrapServersOverride = getBootstrapServers().replace(getHost(), gateway);
        advertisedListeners.add("PLAINTEXT://" + bootstrapServersOverride);
        advertisedListeners.add(brokerAdvertisedListener);

        final Set<Supplier<String>> thisAdvertisedListeners = Reflections.field("advertisedListeners")
                .ofType(Set.class)
                .in(this)
                .get();
        advertisedListeners.addAll(KafkaHelperAccessor.resolveAdvertisedListeners(thisAdvertisedListeners));
        final String kafkaAdvertisedListeners = String.join(",", advertisedListeners);

        String command = "#!/bin/bash\n";
        // exporting KAFKA_ADVERTISED_LISTENERS with the container hostname
        //CHECKSTYLE:OFF
        command += String.format("export KAFKA_ADVERTISED_LISTENERS=%s\n", kafkaAdvertisedListeners);
        //CHECKSTYLE:ON

        final String starterScript = Reflections.staticField("STARTER_SCRIPT")
                .ofType(String.class)
                .in(KafkaContainer.class)
                .get();
        command += "/etc/kafka/docker/run \n";
        copyFileToContainer(Transferable.of(command, 0777), starterScript);
    }

    @Override
    public String getBootstrapServers() {
        if (bootstrapServersOverride != null) {
            return bootstrapServersOverride;
        } else {
            return super.getBootstrapServers();
        }
    }

    public KafkaContainerForNifi setEnvTransient() {
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