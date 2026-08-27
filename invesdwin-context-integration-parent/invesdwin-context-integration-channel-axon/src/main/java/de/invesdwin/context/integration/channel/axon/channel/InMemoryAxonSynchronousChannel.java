package de.invesdwin.context.integration.channel.axon.channel;

import javax.annotation.concurrent.ThreadSafe;

import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore;
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore.Builder;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.serialization.Serializer;

import de.invesdwin.context.integration.channel.axon.channel.serde.ByteArrayAxonSerializer;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;

@ThreadSafe
public class InMemoryAxonSynchronousChannel extends AAxonSynchronousChannel implements ISynchronousChannel {

    private final boolean lowLatency;

    public InMemoryAxonSynchronousChannel(final boolean lowLatency) {
        this.lowLatency = lowLatency;
    }

    public boolean isLowLatency() {
        return lowLatency;
    }

    @Override
    protected Configuration newConfiguration() {
        final EmbeddedEventStore eventStore = newEmbeddedEventStore();
        final Serializer serializer = newSerializer();

        final Configurer configurer = DefaultConfigurer.defaultConfiguration()
                .configureSerializer(c -> serializer)
                .configureMessageSerializer(c -> serializer)
                .configureEventSerializer(c -> serializer)
                .configureEventStore(c -> eventStore);

        return configurer.buildConfiguration();
    }

    protected EmbeddedEventStore newEmbeddedEventStore() {
        return newEmbeddedEventStoreBuilder().build();
    }

    protected Builder newEmbeddedEventStoreBuilder() {
        return EmbeddedEventStore.builder().storageEngine(newEventStorageEngine()).fetchDelay(newFetchDelayMillis());
    }

    protected long newFetchDelayMillis() {
        if (lowLatency) {
            return 0;
        } else {
            return 1;
        }
    }

    protected EventStorageEngine newEventStorageEngine() {
        return new InMemoryEventStorageEngine();
    }

    protected Serializer newSerializer() {
        return ByteArrayAxonSerializer.INSTANCE;
    }

}