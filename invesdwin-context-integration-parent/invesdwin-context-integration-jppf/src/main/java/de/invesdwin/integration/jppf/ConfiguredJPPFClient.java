package de.invesdwin.integration.jppf;

import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.jppf.client.JPPFClient;
import org.jppf.client.concurrent.JPPFExecutorService;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@Named
@Immutable
public final class ConfiguredJPPFClient implements FactoryBean<JPPFClient> {

    public static final JPPFClient INSTANCE;
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final Duration DEFAULT_BATCH_TIMEOUT = new Duration(100, FTimeUnit.MILLISECONDS);

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

    public JPPFExecutorService newBatchedExecutorService() {
        final JPPFExecutorService executorService = new JPPFExecutorService(INSTANCE);
        executorService.setBatchSize(DEFAULT_BATCH_SIZE);
        executorService.setBatchTimeout(DEFAULT_BATCH_TIMEOUT.longValue(FTimeUnit.MILLISECONDS));
        return executorService;
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
