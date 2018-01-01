package de.invesdwin.integration.jppf.client;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.jppf.client.monitoring.topology.TopologyDriver;
import org.jppf.client.monitoring.topology.TopologyEvent;
import org.jppf.client.monitoring.topology.TopologyListener;
import org.jppf.client.monitoring.topology.TopologyManager;
import org.jppf.client.monitoring.topology.TopologyNode;
import org.jppf.management.JMXNodeConnectionWrapper;
import org.jppf.management.JPPFManagementInfo;
import org.jppf.management.JPPFSystemInformation;
import org.jppf.utils.TypedProperties;
import org.jppf.utils.configuration.JPPFProperties;
import org.jppf.utils.configuration.JPPFProperty;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.integration.jppf.ATopologyVisitor;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.time.fdate.FTimeUnit;

@ThreadSafe
public class ProcessingThreadsCounter {

    private static final long CONNECT_TIMEOUT_MILLIS = ContextProperties.DEFAULT_NETWORK_TIMEOUT
            .longValue(FTimeUnit.MILLISECONDS);

    @GuardedBy("this")
    private int processingThreadsCount;
    @GuardedBy("this")
    private int nodesCount;
    private final TopologyManager topologyManager;

    public ProcessingThreadsCounter(final TopologyManager topologyManager) {
        this.topologyManager = topologyManager;
        topologyManager.addTopologyListener(new TopologyListener() {
            @Override
            public void nodeUpdated(final TopologyEvent event) {
                //ignore
            }

            @Override
            public void nodeRemoved(final TopologyEvent event) {
                synchronized (ProcessingThreadsCounter.this) {
                    processingThreadsCount -= extractProcessingThreads(
                            event.getNodeOrPeer().getManagementInfo().getSystemInfo());
                    nodesCount--;
                    Assertions.assertThat(processingThreadsCount).isGreaterThanOrEqualTo(0);
                    Assertions.assertThat(nodesCount).isGreaterThanOrEqualTo(0);
                }
            }

            @Override
            public void nodeAdded(final TopologyEvent event) {
                synchronized (ProcessingThreadsCounter.this) {
                    processingThreadsCount += extractProcessingThreads(
                            event.getNodeOrPeer().getManagementInfo().getSystemInfo());
                    nodesCount++;
                }
            }

            @Override
            public void driverUpdated(final TopologyEvent event) {
                //ignore
            }

            @Override
            public void driverRemoved(final TopologyEvent event) {
                synchronized (ProcessingThreadsCounter.this) {
                    final Pair<Integer, Integer> processingThreadsAndNodes = countProcessingThreads();
                    processingThreadsCount = processingThreadsAndNodes.getFirst();
                    nodesCount = processingThreadsAndNodes.getSecond();
                }
            }

            @Override
            public void driverAdded(final TopologyEvent event) {
                synchronized (ProcessingThreadsCounter.this) {
                    final Pair<Integer, Integer> processingThreadsAndNodes = countProcessingThreads();
                    processingThreadsCount = processingThreadsAndNodes.getFirst();
                    nodesCount = processingThreadsAndNodes.getSecond();
                }
            }
        });
        synchronized (this) {
            final Pair<Integer, Integer> processingThreadsAndNodes = countProcessingThreads();
            processingThreadsCount = processingThreadsAndNodes.getFirst();
            nodesCount = processingThreadsAndNodes.getSecond();
        }
    }

    private Pair<Integer, Integer> countProcessingThreads() {
        final AtomicInteger processingThreads = new AtomicInteger(0);
        final AtomicInteger nodes = new AtomicInteger(0);
        new ATopologyVisitor() {
            @Override
            protected void visitNode(final TopologyNode node) {
                final JPPFManagementInfo managementInfo = node.getManagementInfo();
                final JPPFSystemInformation systemInfo = extractSystemInfo(managementInfo);
                if (systemInfo != null) {
                    processingThreads.addAndGet(extractProcessingThreads(systemInfo));
                    nodes.incrementAndGet();
                }
            }

            @Override
            protected void visitDriver(final TopologyDriver driver) {
                final JPPFSystemInformation systemInfo = extractSystemInfo(driver.getManagementInfo());
                System.out.println(systemInfo);
            }
        }.process(topologyManager);
        return Pair.of(processingThreads.get(), nodes.get());
    }

    private JPPFSystemInformation extractSystemInfo(final JPPFManagementInfo managementInfo) {
        if (managementInfo.getSystemInfo() != null) {
            return managementInfo.getSystemInfo();
        }
        final String host = managementInfo.getHost();
        final int port = managementInfo.getPort();
        final boolean secure = managementInfo.isSecure();
        final JMXNodeConnectionWrapper jmx = new JMXNodeConnectionWrapper(host, port, secure);
        try {
            jmx.connectAndWait(CONNECT_TIMEOUT_MILLIS);
            if (!jmx.isConnected()) {
                throw new TimeoutException("JMX connect timeout [" + CONNECT_TIMEOUT_MILLIS + " ms] exceeded for: host="
                        + host + " port=" + port + " secure=" + secure);
            }
            final JPPFSystemInformation systemInfo = jmx.systemInformation();
            return systemInfo;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                jmx.close();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Integer extractProcessingThreads(final JPPFSystemInformation nodeConfig) {
        // get the number of processing threads in the node
        final TypedProperties jppf = nodeConfig.getJppf();
        final boolean isPeer = jppf.getBoolean("jppf.peer.driver", false);
        final JPPFProperty<Integer> prop = isPeer ? JPPFProperties.PEER_PROCESSING_THREADS
                : JPPFProperties.PROCESSING_THREADS;
        int nbThreads = jppf.getInt(prop.getName(), -1);
        // if number of threads is not defined, we assume it is the number of available processors
        if (nbThreads <= 0) {
            nbThreads = nodeConfig.getRuntime().getInt("availableProcessors");
        }
        if (nbThreads <= 0) {
            nbThreads = 1;
        }
        return nbThreads;
    }

    public synchronized int getProcessingThreadsCount() {
        return processingThreadsCount;
    }

    public int getNodesCount() {
        return nodesCount;
    }

    public TopologyManager getTopologyManager() {
        return topologyManager;
    }

}
