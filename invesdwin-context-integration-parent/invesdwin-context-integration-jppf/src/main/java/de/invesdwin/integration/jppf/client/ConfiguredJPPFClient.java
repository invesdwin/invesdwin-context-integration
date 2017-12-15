package de.invesdwin.integration.jppf.client;

import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.jppf.client.JPPFClient;
import org.jppf.client.concurrent.JPPFExecutorService;
import org.jppf.client.monitoring.topology.TopologyManager;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.integration.jppf.JPPFClientProperties;
import de.invesdwin.integration.jppf.ProcessingThreadsCounter;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@Named
@Immutable
public final class ConfiguredJPPFClient implements FactoryBean<JPPFClient> {

    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final Duration DEFAULT_BATCH_TIMEOUT = new Duration(100, FTimeUnit.MILLISECONDS);
    private static JPPFClient instance;

    private static TopologyManager topologyManager;
    private static ProcessingThreadsCounter processingThreadsCounter;
    private static JPPFExecutorService executorService;

    public static synchronized JPPFExecutorService getBatchedExecutorService() {
        if (executorService == null) {
            executorService = new JPPFExecutorService(getInstance());
            executorService.setBatchSize(DEFAULT_BATCH_SIZE);
            executorService.setBatchTimeout(DEFAULT_BATCH_TIMEOUT.longValue(FTimeUnit.MILLISECONDS));
        }
        return executorService;
    }

    private static synchronized ProcessingThreadsCounter getProcessingThreadsCounter() {
        if (processingThreadsCounter == null) {
            processingThreadsCounter = new ProcessingThreadsCounter(getTopologyManager());
        }
        return processingThreadsCounter;
    }

    public static int getProcessingThreads() {
        return getProcessingThreadsCounter().getProcessingThreads();
    }

    public static synchronized TopologyManager getTopologyManager() {
        if (topologyManager == null) {
            topologyManager = new TopologyManager(getInstance());
        }
        return topologyManager;
    }

    @Override
    public JPPFClient getObject() throws Exception {
        return getInstance();
    }

    public static synchronized JPPFClient getInstance() {
        if (instance == null) {
            Assertions.checkTrue(JPPFClientProperties.INITIALIZED);
            instance = new JPPFClient();
            instance.addDriverDiscovery(new ConfiguredClientDriverDiscovery());
            final ExecutorService executor = Executors
                    .newFixedThreadPool(ConfiguredJPPFClient.class.getSimpleName() + "_INIT", 1);
            instance.addClientQueueListener(new ConnectionSizingClientQueueListener());
            executor.shutdown();
        }
        return instance;
    }

    @Override
    public Class<?> getObjectType() {
        return ConfiguredJPPFClient.class;
    }

}
