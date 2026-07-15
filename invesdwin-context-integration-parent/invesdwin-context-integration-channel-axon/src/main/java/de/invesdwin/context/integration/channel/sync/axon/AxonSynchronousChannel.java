package de.invesdwin.context.integration.channel.sync.axon;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.AxonServerConfiguration.Builder;
import org.axonframework.axonserver.connector.AxonServerConnectionManager;
import org.axonframework.axonserver.connector.event.axon.AxonServerEventStore;
import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;

import com.thoughtworks.xstream.XStream;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;

@ThreadSafe
public class AxonSynchronousChannel implements ISynchronousChannel {

    private final String serverUrl;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();
    private final AxonSynchronousChannelFinalizer finalizer;

    public AxonSynchronousChannel(final String serverUrl) {
        this.serverUrl = serverUrl;
        this.finalizer = new AxonSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    public Configuration getConfiguration() {
        return finalizer.configuration;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    @Override
    public synchronized void open() throws IOException {
        if (activeCount.incrementAndGet() != 1) {
            return;
        }
        finalizer.configuration = newConfiguration();
        finalizer.configuration.start();
    }

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

    protected Serializer newSerializer() {
        final XStream xStream = new XStream();
        xStream.allowTypes(new Class[] { AxonChannelMessage.class });
        final Serializer serializer = XStreamSerializer.builder().xStream(xStream).build();
        return serializer;
    }

    protected AxonServerConnectionManager newAxonServerConnectionManager(
            final AxonServerConfiguration axonServerConfiguration) {
        return newAxonServerConnectionManagerBuilder(axonServerConfiguration).build();
    }

    protected org.axonframework.axonserver.connector.AxonServerConnectionManager.Builder newAxonServerConnectionManagerBuilder(
            final AxonServerConfiguration axonServerConfiguration) {
        return AxonServerConnectionManager.builder().axonServerConfiguration(axonServerConfiguration);
    }

    protected AxonServerConfiguration newAxonServerConfiguration() {
        return newAxonServerConfigurationBuilder().build();
    }

    protected Builder newAxonServerConfigurationBuilder() {
        return AxonServerConfiguration.builder().servers(serverUrl);
    }

    @Override
    public synchronized void close() throws IOException {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        if (activeCountBefore == 1) {
            finalizer.close();
        }
    }

    private static final class AxonSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile Configuration configuration;

        @Override
        protected void clean() {
            final Configuration configCopy = configuration;
            if (configCopy != null) {
                configuration = null;
                configCopy.shutdown();
            }
        }

        @Override
        protected boolean isCleaned() {
            return configuration == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }
    }
}