package de.invesdwin.integration.jppf.client;

import java.net.URL;
import java.net.URLClassLoader;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.jppf.client.JPPFClient;
import org.jppf.client.monitoring.jobs.JobMonitor;
import org.jppf.client.monitoring.topology.TopologyManager;
import org.jppf.location.MemoryLocation;
import org.jppf.location.URLLocation;
import org.jppf.node.protocol.ClassPath;
import org.springframework.beans.factory.FactoryBean;

import de.invesdwin.integration.jppf.JPPFClientProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

@Named
@Immutable
public final class ConfiguredJPPFClient implements FactoryBean<JPPFClient> {

    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final Duration DEFAULT_BATCH_TIMEOUT = new Duration(100, FTimeUnit.MILLISECONDS);
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

    private static synchronized ProcessingThreadsCounter getProcessingThreadsCounter() {
        if (processingThreadsCounter == null) {
            processingThreadsCounter = new ProcessingThreadsCounter(getTopologyManager());
        }
        return processingThreadsCounter;
    }

    public static int getProcessingThreadsCount() {
        return getProcessingThreadsCounter().getProcessingThreadsCount();
    }

    public static int getNodesCount() {
        return getProcessingThreadsCounter().getNodesCount();
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
            instance = new JPPFClient();
            topologyManager = new TopologyManager(instance);
            instance.addDriverDiscovery(new ConfiguredClientDriverDiscovery());
            instance.addClientQueueListener(new ConnectionSizingClientQueueListener());

            while (topologyManager.getNodeCount() == 0) {
                try {
                    FTimeUnit.SECONDS.sleep(1);
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return instance;
    }

    @Override
    public Class<?> getObjectType() {
        return ConfiguredJPPFClient.class;
    }

    public static void addContextJarsToClassPath(final ClassPath classPath) {
        final URLClassLoader classLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
        for (final URL url : classLoader.getURLs()) {
            final String urlStr = url.toString();
            if (urlStr.endsWith(".jar")) {
                try {
                    final MemoryLocation memory = new MemoryLocation(new URLLocation(url).toByteArray());
                    classPath.add(urlStr, memory);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}
