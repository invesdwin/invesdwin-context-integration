package de.invesdwin.integration.jppf;

import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

import org.jppf.client.JPPFClient;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Executors;

@Named
@NotThreadSafe
public final class ConfiguredJPPFClient implements FactoryBean<JPPFClient> {

    public static final JPPFClient INSTANCE;

    static {
        Assertions.checkTrue(JPPFClientProperties.INITIALIZED);
        INSTANCE = new JPPFClient();
        INSTANCE.addDriverDiscovery(new ConfiguredClientDriverDiscovery());
        final ExecutorService executor = Executors
                .newFixedThreadPool(ConfiguredJPPFClient.class.getSimpleName() + "_INIT", 1);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                INSTANCE.addClientQueueListener(
                        new ConnectionSizingClientQueueListener(INSTANCE.awaitWorkingConnectionPool()));
            }
        });
        executor.shutdown();
    }

    private ConfiguredJPPFClient() {
        super();
    }

    @Override
    public JPPFClient getObject() throws Exception {
        return INSTANCE;
    }

    @Override
    public Class<?> getObjectType() {
        return ConfiguredJPPFClient.class;
    }

}
