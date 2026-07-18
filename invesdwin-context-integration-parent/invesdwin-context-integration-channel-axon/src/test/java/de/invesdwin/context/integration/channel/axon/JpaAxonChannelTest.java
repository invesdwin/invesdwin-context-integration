package de.invesdwin.context.integration.channel.axon;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.axon.channel.AAxonSynchronousChannel;
import de.invesdwin.context.integration.channel.axon.channel.JpaAxonSynchronousChannel;
import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.context.persistence.jpa.PersistenceUnitContext;
import de.invesdwin.context.persistence.jpa.test.PersistenceTest;
import de.invesdwin.context.persistence.jpa.test.PersistenceTestContext;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;

@PersistenceTest(PersistenceTestContext.MEMORY)
@NotThreadSafe
public class JpaAxonChannelTest extends AAxonChannelTest {

    private final ALoadingCache<Boolean, JpaAxonSynchronousChannel> server_channel = new ALoadingCache<Boolean, JpaAxonSynchronousChannel>() {
        @Override
        protected JpaAxonSynchronousChannel loadValue(final Boolean key) {
            final PersistenceUnitContext persistenceUnitContext = PersistenceProperties
                    .getPersistenceUnitContext(PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME);
            return new JpaAxonSynchronousChannel(persistenceUnitContext.getEntityManager(),
                    persistenceUnitContext.getTransactionManager(), false);
        }
    };
    private final ALoadingCache<Boolean, JpaAxonSynchronousChannel> server_channelLowLatency = new ALoadingCache<Boolean, JpaAxonSynchronousChannel>() {
        @Override
        protected JpaAxonSynchronousChannel loadValue(final Boolean key) {
            final PersistenceUnitContext persistenceUnitContext = PersistenceProperties
                    .getPersistenceUnitContext(PersistenceProperties.DEFAULT_PERSISTENCE_UNIT_NAME);
            return new JpaAxonSynchronousChannel(persistenceUnitContext.getEntityManager(),
                    persistenceUnitContext.getTransactionManager(), true);
        }
    };

    @Override
    protected AAxonSynchronousChannel getAxonSynchronousChannel(final boolean server, final String topic,
            final boolean lowLatency) {
        if (lowLatency) {
            return server_channelLowLatency.get(server);
        } else {
            return server_channel.get(server);
        }
    }
}
