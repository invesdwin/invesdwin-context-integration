package de.invesdwin.context.integration.grid.ignite2.client;

import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.integration.grid.ignite2.Ignite2ClientProperties;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import jakarta.inject.Named;

@Named
@Immutable
public final class ConfiguredIgnite2Client implements FactoryBean<IgniteClient> {

    public static final Duration DEFAULT_BATCH_TIMEOUT = new Duration(3, FTimeUnit.SECONDS);
    private static final Log LOG = new Log(ConfiguredIgnite2Client.class);
    private static IgniteClient instance;

    private static Ignite2ClientProcessingThreadsCounter processingThreadsCounter;

    public static synchronized Ignite2ClientProcessingThreadsCounter getProcessingThreadsCounter() {
        if (processingThreadsCounter == null) {
            processingThreadsCounter = new Ignite2ClientProcessingThreadsCounter(getInstance());
        }
        return processingThreadsCounter;
    }

    @Override
    public IgniteClient getObject() throws Exception {
        return getInstance();
    }

    public static synchronized IgniteClient getInstance() {
        if (instance == null) {
            Assertions.checkTrue(Ignite2ClientProperties.INITIALIZED);

            final ClientConfiguration cfg = new ClientConfiguration();
            cfg.setAddressesFinder(new ConfiguredClientAddressFinder());

            final IgniteClient startedClient = Ignition.startClient(cfg);
            setInstance(startedClient);
        }
        return instance;
    }

    private static synchronized void setInstance(final IgniteClient client) {
        Assertions.checkNull(instance, "already started");
        instance = client;
        if (client != null) {
            waitForWarmup();
            LOG.info("%s connected", ConfiguredIgnite2Client.class.getSimpleName());
        }
    }

    private static void waitForWarmup() {
        final Ignite2ClientProcessingThreadsCounter processingThreadsCounterCopy = getProcessingThreadsCounter();
        try {
            processingThreadsCounterCopy.waitForMinimumCounts(1, Duration.ONE_MINUTE);
        } catch (final TimeoutException e) {
            //ignore
        }
        processingThreadsCounterCopy.logWarmupFinished();
    }

    @Override
    public Class<?> getObjectType() {
        return IgniteClient.class;
    }

    public static synchronized void reset() {
        if (instance != null) {
            try {
                instance.close();
            } catch (final Exception e) {
                LOG.error("Error shutting down Ignite Thin Client", e);
            }
            instance = null;
            processingThreadsCounter = null;
            LOG.info("%s stopped", ConfiguredIgnite2Client.class.getSimpleName());
        }
    }
}