package de.invesdwin.integration.jppf;

import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.jppf.client.monitoring.topology.TopologyDriver;
import org.jppf.client.monitoring.topology.TopologyEvent;
import org.jppf.client.monitoring.topology.TopologyListener;
import org.jppf.client.monitoring.topology.TopologyManager;
import org.jppf.client.monitoring.topology.TopologyNode;
import org.jppf.management.JPPFSystemInformation;
import org.jppf.utils.TypedProperties;
import org.jppf.utils.configuration.JPPFProperties;
import org.jppf.utils.configuration.JPPFProperty;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.tuple.Pair;

@ThreadSafe
public class ProcessingThreadsCounter {

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
                    final Pair<Integer, Integer> processingThreadsAndNodes = countProcessingThreads(topologyManager);
                    processingThreadsCount = processingThreadsAndNodes.getFirst();
                    nodesCount = processingThreadsAndNodes.getSecond();
                }
            }

            @Override
            public void driverAdded(final TopologyEvent event) {
                synchronized (ProcessingThreadsCounter.this) {
                    final Pair<Integer, Integer> processingThreadsAndNodes = countProcessingThreads(topologyManager);
                    processingThreadsCount = processingThreadsAndNodes.getFirst();
                    nodesCount = processingThreadsAndNodes.getSecond();
                }
            }
        });
        synchronized (this) {
            final Pair<Integer, Integer> processingThreadsAndNodes = countProcessingThreads(topologyManager);
            processingThreadsCount = processingThreadsAndNodes.getFirst();
            nodesCount = processingThreadsAndNodes.getSecond();
        }
    }

    private Pair<Integer, Integer> countProcessingThreads(final TopologyManager manager) {
        final AtomicInteger processingThreads = new AtomicInteger(0);
        final AtomicInteger nodes = new AtomicInteger(0);
        new ATopologyVisitor() {
            @Override
            protected void visitNode(final TopologyNode node) {
                final JPPFSystemInformation systemInfo = node.getManagementInfo().getSystemInfo();
                processingThreads.addAndGet(extractProcessingThreads(systemInfo));
                nodes.incrementAndGet();
            }

            @Override
            protected void visitDriver(final TopologyDriver driver) {
                //ignore
            }
        }.process(manager);
        return Pair.of(processingThreads.get(), nodes.get());
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
