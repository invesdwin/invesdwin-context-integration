package de.invesdwin.context.integration.channel.sync.axon.channel;

import javax.annotation.concurrent.ThreadSafe;

import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore;
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore.Builder;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;

import com.thoughtworks.xstream.XStream;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;

@ThreadSafe
public class InMemoryAxonSynchronousChannel extends AAxonSynchronousChannel implements ISynchronousChannel {

    public InMemoryAxonSynchronousChannel() {}

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
        return EmbeddedEventStore.builder().storageEngine(newEventStorageEngine()).fetchDelay(1);
    }

    protected EventStorageEngine newEventStorageEngine() {
        return new InMemoryEventStorageEngine();
    }

    protected Serializer newSerializer() {
        final XStream xStream = new XStream();
        final Serializer serializer = XStreamSerializer.builder().xStream(xStream).build();
        return serializer;
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