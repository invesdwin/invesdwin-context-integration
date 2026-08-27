package de.invesdwin.context.integration.channel.axon.channel;

import javax.annotation.concurrent.ThreadSafe;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.AxonServerConfiguration.Builder;
import org.axonframework.axonserver.connector.AxonServerConnectionManager;
import org.axonframework.axonserver.connector.event.axon.AxonServerEventStore;
import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.serialization.Serializer;

import de.invesdwin.context.integration.channel.axon.channel.serde.ByteArrayAxonSerializer;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;

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
                .configureEventStore(c -> newAxonEventStore(axonServerConfiguration, connectionManager, c));
        return configurer.buildConfiguration();
    }

    protected AxonServerEventStore newAxonEventStore(final AxonServerConfiguration axonServerConfiguration,
            final AxonServerConnectionManager connectionManager, final Configuration configuration) {
        return newAxonServerEventStoreBuilder(axonServerConfiguration, connectionManager, configuration).build();
    }

    protected AxonServerEventStore.Builder newAxonServerEventStoreBuilder(
            final AxonServerConfiguration axonServerConfiguration, final AxonServerConnectionManager connectionManager,
            final Configuration configuration) {
        return AxonServerEventStore.builder()
                .configuration(axonServerConfiguration)
                .platformConnectionManager(connectionManager)
                .snapshotFilter(configuration.snapshotFilter())
                .eventSerializer(configuration.serializer())
                .snapshotSerializer(configuration.serializer());
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
        Builder builder = AxonServerConfiguration.builder().servers(serverUrl);
        final String context = newContext();
        if (context != null) {
            builder = builder.context(context);
        }
        return builder;
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
        return null;
    }

}