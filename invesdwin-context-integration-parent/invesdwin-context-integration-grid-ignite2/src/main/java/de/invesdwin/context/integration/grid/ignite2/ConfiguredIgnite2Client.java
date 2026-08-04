package de.invesdwin.context.integration.grid.ignite2;

import javax.annotation.concurrent.Immutable;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import jakarta.inject.Named;

@Named
@Immutable
public final class ConfiguredIgnite2Client implements FactoryBean<IgniteClient> {

    private static final Log LOG = new Log(ConfiguredIgnite2Client.class);
    private static IgniteClient instance;

    @Override
    public IgniteClient getObject() throws Exception {
        return getInstance();
    }

    public static synchronized IgniteClient getInstance() {
        if (instance == null) {
            Assertions.checkTrue(Ignite2ClientProperties.INITIALIZED);

            final ClientConfiguration cfg = new ClientConfiguration();
            cfg.setAddressesFinder(new ConfiguredClientAddressFinder());

            // Note: Ignition.startClient() acts as its own "warmup" by blocking
            // until the socket connection to the server is successfully established.
            instance = Ignition.startClient(cfg);

            LOG.info("%s connected", ConfiguredIgnite2Client.class.getSimpleName());
        }
        return instance;
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
            LOG.info("%s stopped", ConfiguredIgnite2Client.class.getSimpleName());
        }
    }
}