package de.invesdwin.integration.jppf;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

import org.jppf.client.JPPFClient;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.util.assertions.Assertions;

@Named
@NotThreadSafe
public final class ConfiguredJPPFClient extends JPPFClient implements FactoryBean<ConfiguredJPPFClient> {

    public static final ConfiguredJPPFClient INSTANCE;

    static {
        Assertions.checkNotNull(JPPFClientProperties.class);
        INSTANCE = new ConfiguredJPPFClient();
        INSTANCE.addDriverDiscovery(new ConfiguredClientDriverDiscovery());
        INSTANCE.addClientQueueListener(new ConnectionSizingClientQueueListener(INSTANCE.awaitWorkingConnectionPool()));
    }

    private ConfiguredJPPFClient() {
        super();
    }

    @Override
    public ConfiguredJPPFClient getObject() throws Exception {
        return INSTANCE;
    }

    @Override
    public Class<?> getObjectType() {
        return ConfiguredJPPFClient.class;
    }

}
