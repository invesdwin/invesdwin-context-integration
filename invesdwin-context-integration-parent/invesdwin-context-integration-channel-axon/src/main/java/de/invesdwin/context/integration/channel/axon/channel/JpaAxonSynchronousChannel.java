package de.invesdwin.context.integration.channel.axon.channel;

import javax.annotation.concurrent.ThreadSafe;

import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.jpa.SimpleEntityManagerProvider;
import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore;
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore.Builder;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.jpa.JpaEventStorageEngine;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.axonframework.spring.messaging.unitofwork.SpringTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import com.thoughtworks.xstream.XStream;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import jakarta.persistence.EntityManager;

@ThreadSafe
public class JpaAxonSynchronousChannel extends AAxonSynchronousChannel implements ISynchronousChannel {

    private final EntityManagerProvider entityManagerProvider;
    private final org.axonframework.common.transaction.TransactionManager transactionManager;
    private final boolean lowLatency;

    public JpaAxonSynchronousChannel(final EntityManager entityManager,
            final PlatformTransactionManager transactionManager, final boolean lowLatency) {
        this(new SimpleEntityManagerProvider(entityManager), new SpringTransactionManager(transactionManager),
                lowLatency);
    }

    public JpaAxonSynchronousChannel(final EntityManagerProvider entityManagerProvider,
            final org.axonframework.common.transaction.TransactionManager transactionManager,
            final boolean lowLatency) {
        this.entityManagerProvider = entityManagerProvider;
        this.transactionManager = transactionManager;
        this.lowLatency = lowLatency;
    }

    public boolean isLowLatency() {
        return lowLatency;
    }

    @Override
    protected Configuration newConfiguration() {
        final Serializer serializer = newSerializer();

        final Configurer configurer = DefaultConfigurer.defaultConfiguration()
                .configureSerializer(c -> serializer)
                .configureMessageSerializer(c -> serializer)
                .configureEventSerializer(c -> serializer)
                .configureEventStore(c -> newEmbeddedEventStore(c));

        return configurer.buildConfiguration();
    }

    protected EmbeddedEventStore newEmbeddedEventStore(final Configuration configuration) {
        return newEmbeddedEventStoreBuilder(configuration).build();
    }

    protected Builder newEmbeddedEventStoreBuilder(final Configuration configuration) {
        return EmbeddedEventStore.builder()
                .storageEngine(newEventStorageEngine(configuration))
                .fetchDelay(newFetchDelayMillis());
    }

    protected long newFetchDelayMillis() {
        if (lowLatency) {
            return 0;
        } else {
            return 1;
        }
    }

    protected EventStorageEngine newEventStorageEngine(final Configuration configuration) {
        return JpaEventStorageEngine.builder()
                .entityManagerProvider(entityManagerProvider)
                .transactionManager(transactionManager)
                .eventSerializer(configuration.serializer())
                .snapshotSerializer(configuration.serializer())
                .build();
    }

    protected Serializer newSerializer() {
        final XStream xStream = new XStream();
        final Serializer serializer = XStreamSerializer.builder().xStream(xStream).build();
        return serializer;
    }

}