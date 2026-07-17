package de.invesdwin.context.integration.channel.sync.axon.channel;

import javax.annotation.concurrent.ThreadSafe;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.AxonServerConnectionManager;
import org.axonframework.axonserver.connector.event.axon.AxonServerEventStore;
import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.serialization.Serializer;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.axon.channel.serde.ByteArrayAxonSerializer;

@ThreadSafe
public class ServerAxonSynchronousChannel extends AAxonSynchronousChannel implements ISynchronousChannel {

    private final String serverUrl;

    public ServerAxonSynchronousChannel(final String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    @Override
    protected Configuration newConfiguration() {
        // Configure connection specifically for this isolated context
        final AxonServerConfiguration axonServerConfiguration = newAxonServerConfiguration();
        final AxonServerConnectionManager connectionManager = newAxonServerConnectionManager(axonServerConfiguration);
        final Serializer serializer = newSerializer();
        final Configurer configurer = DefaultConfigurer.defaultConfiguration()
                .configureSerializer(c -> serializer)
                .configureMessageSerializer(c -> serializer)
                .configureEventSerializer(c -> serializer)
                .registerComponent(AxonServerConfiguration.class, c -> axonServerConfiguration)
                .registerComponent(AxonServerConnectionManager.class, c -> connectionManager)
                .configureEventStore(c -> AxonServerEventStore.builder()
                        .configuration(axonServerConfiguration)
                        .platformConnectionManager(connectionManager)
                        .snapshotFilter(c.snapshotFilter())
                        .eventSerializer(c.serializer())
                        .snapshotSerializer(c.serializer())
                        .build());
        return configurer.buildConfiguration();
    }

    protected AxonServerConnectionManager newAxonServerConnectionManager(
            final AxonServerConfiguration axonServerConfiguration) {
        return newAxonServerConnectionManagerBuilder(axonServerConfiguration).build();
    }

    protected AxonServerConnectionManager.Builder newAxonServerConnectionManagerBuilder(
            final AxonServerConfiguration axonServerConfiguration) {
        return AxonServerConnectionManager.builder().axonServerConfiguration(axonServerConfiguration);
    }

    protected AxonServerConfiguration newAxonServerConfiguration() {
        return newAxonServerConfigurationBuilder().build();
    }

    protected AxonServerConfiguration.Builder newAxonServerConfigurationBuilder() {
        return AxonServerConfiguration.builder().servers(serverUrl).context(newContext());
    }

    protected Serializer newSerializer() {
        return ByteArrayAxonSerializer.INSTANCE;
    }

    /**
     * If you have axon enterprise, you can define a different context for each channel to separate the events (though
     * this requires provisioning via e.g. ContextAdminServiceGrpc. Otherwise you have to use the default context and
     * separate the messages in the reader/writer.
     */
    protected String newContext() {
        return "default";
    }

}