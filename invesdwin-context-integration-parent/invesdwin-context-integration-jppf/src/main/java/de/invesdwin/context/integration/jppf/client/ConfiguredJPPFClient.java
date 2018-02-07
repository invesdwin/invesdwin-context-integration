package de.invesdwin.context.integration.jppf.client;

import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.jppf.client.JPPFClient;
import org.jppf.client.monitoring.jobs.JobMonitor;
import org.jppf.client.monitoring.topology.TopologyManager;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@Named
@Immutable
public final class ConfiguredJPPFClient implements FactoryBean<JPPFClient> {

    public static final Duration DEFAULT_BATCH_TIMEOUT = new Duration(3, FTimeUnit.SECONDS);
    private static JPPFClient instance;

    private static TopologyManager topologyManager;
    private static JobMonitor jobMonitor;
    private static JPPFProcessingThreadsCounter processingThreadsCounter;
    private static ConfiguredJPPFExecutorService executorService;

    public static synchronized ConfiguredJPPFExecutorService getBatchedExecutorService() {
        if (executorService == null) {
            executorService = new ConfiguredJPPFExecutorService(getInstance());
            executorService.setBatchTimeout(DEFAULT_BATCH_TIMEOUT.longValue(FTimeUnit.MILLISECONDS));
        }
        return executorService;
    }

    public static synchronized JPPFProcessingThreadsCounter getProcessingThreadsCounter() {
        if (processingThreadsCounter == null) {
            processingThreadsCounter = new JPPFProcessingThreadsCounter(getTopologyManager());
        }
        return processingThreadsCounter;
    }

    public static synchronized TopologyManager getTopologyManager() {
        if (topologyManager == null) {
            Assertions.checkNotNull(getInstance());
            Assertions.checkNotNull(topologyManager);
        }
        return topologyManager;
    }

    public static synchronized JobMonitor getJobMonitor() {
        if (jobMonitor == null) {
            jobMonitor = new JobMonitor(getTopologyManager());
        }
        return jobMonitor;
    }

    @Override
    public JPPFClient getObject() throws Exception {
        return getInstance();
    }

    public static synchronized JPPFClient getInstance() {
        if (instance == null) {
            Assertions.checkTrue(JPPFClientProperties.INITIALIZED);
            JPPFClientProperties.fixSystemProperties();
            instance = new JPPFClient();
            topologyManager = new TopologyManager(instance);
            Assertions.checkNotNull(getProcessingThreadsCounter());
            final ConfiguredClientDriverDiscovery clientDiscovery = new ConfiguredClientDriverDiscovery();
            instance.addDriverDiscovery(clientDiscovery);
            instance.addClientQueueListener(new ConnectionSizingClientQueueListener());

            waitForWarmup(clientDiscovery);
        }
        return instance;
    }

    private static void waitForWarmup(final ConfiguredClientDriverDiscovery clientDiscovery) {
        final int expectedDriversCount = clientDiscovery.getDestinationProvider().getDestinations().size();
        int minimumNodesCount = 0;
        if (JPPFClientProperties.LOCAL_EXECUTION_ENABLED) {
            minimumNodesCount += 1;
        }
        if (expectedDriversCount > 0) {
            minimumNodesCount += 1;
        }
        try {
            processingThreadsCounter.waitForMinimumCounts(expectedDriversCount, minimumNodesCount, Duration.ONE_MINUTE);
        } catch (final TimeoutException e) {
            //ignore
        }
        processingThreadsCounter.logWarmupFinished();
    }

    @Override
    public Class<?> getObjectType() {
        return ConfiguredJPPFClient.class;
    }

}
