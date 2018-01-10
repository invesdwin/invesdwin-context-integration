package de.invesdwin.context.integration.jppf.client;

import java.util.Enumeration;
import java.util.Properties;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.jppf.client.JPPFClient;
import org.jppf.client.monitoring.jobs.JobMonitor;
import org.jppf.client.monitoring.topology.TopologyManager;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@Named
@Immutable
public final class ConfiguredJPPFClient implements FactoryBean<JPPFClient> {

    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final Duration DEFAULT_BATCH_TIMEOUT = new Duration(100, FTimeUnit.MILLISECONDS);
    private static final Log LOG = new Log(ConfiguredJPPFClient.class);
    private static JPPFClient instance;

    private static TopologyManager topologyManager;
    private static JobMonitor jobMonitor;
    private static ProcessingThreadsCounter processingThreadsCounter;
    private static ConfiguredJPPFExecutorService executorService;

    public static synchronized ConfiguredJPPFExecutorService getBatchedExecutorService() {
        if (executorService == null) {
            executorService = new ConfiguredJPPFExecutorService(getInstance());
            executorService.setBatchSize(DEFAULT_BATCH_SIZE);
            executorService.setBatchTimeout(DEFAULT_BATCH_TIMEOUT.longValue(FTimeUnit.MILLISECONDS));
        }
        return executorService;
    }

    public static synchronized ProcessingThreadsCounter getProcessingThreadsCounter() {
        if (processingThreadsCounter == null) {
            processingThreadsCounter = new ProcessingThreadsCounter(getTopologyManager());
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
            fixSystemProperties();
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
        int triesLeft = 10;
        do {
            try {
                FTimeUnit.SECONDS.sleep(1);
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
            processingThreadsCounter.refresh();
            triesLeft--;
        } while ((processingThreadsCounter.getDriversCount() != expectedDriversCount
                || processingThreadsCounter.getNodesCount() < minimumNodesCount) && triesLeft > 0);
        logWarmupFinished();
    }

    private static void logWarmupFinished() {
        final StringBuilder message = new StringBuilder();
        message.append(JPPFClient.class.getSimpleName());
        message.append(" detected ");
        message.append(processingThreadsCounter.getDriversCount());
        message.append(" driver");
        if (processingThreadsCounter.getDriversCount() != 1) {
            message.append("s");
        }
        message.append(" for ");
        message.append(processingThreadsCounter.getNodesCount());
        message.append(" node");
        if (processingThreadsCounter.getNodesCount() != 1) {
            message.append("s");
        }
        message.append(" with ");
        message.append(processingThreadsCounter.getProcessingThreadsCount());
        message.append(" processing thread");
        if (processingThreadsCounter.getProcessingThreadsCount() != 1) {
            message.append("s");
        }
        if (JPPFClientProperties.LOCAL_EXECUTION_ENABLED) {
            message.append(" (of which 1 local node contributes ");
            message.append(JPPFClientProperties.LOCAL_EXECUTION_THREADS);
            message.append(" processing thread");
            if (JPPFClientProperties.LOCAL_EXECUTION_THREADS != 1) {
                message.append("s");
            }
            message.append(")");
        }
        LOG.info("%s", message);
    }

    @Override
    public Class<?> getObjectType() {
        return ConfiguredJPPFClient.class;
    }

    //TODO: remove as soon as this is fixed: http://www.jppf.org/tracker/tbg/jppf/issues/JPPF-523
    private static void fixSystemProperties() {
        //CHECKSTYLE:OFF
        final Properties sysProps = System.getProperties();
        //CHECKSTYKE:ON
        final Enumeration<?> en = sysProps.propertyNames();
        while (en.hasMoreElements()) {
            final String name = (String) en.nextElement();
            final String value = sysProps.getProperty(name);
            if (value == null) {
                sysProps.setProperty(name, "");
            }
        }
    }

}
