package de.invesdwin.context.integration.jppf.client;

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

import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.jppf.topology.ATopologyVisitor;
import de.invesdwin.context.integration.jppf.topology.TopologyNodes;
import de.invesdwin.util.bean.tuple.Triple;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;

@ThreadSafe
public class ProcessingThreadsCounter {

    private final TopologyManager topologyManager;
    @GuardedBy("this")
    private int processingThreadsCount;
    @GuardedBy("this")
    private int driversCount;
    @GuardedBy("this")
    private int nodesCount;
    @GuardedBy("this")
    private FDate lastRefresh = FDate.MIN_DATE;

    public ProcessingThreadsCounter(final TopologyManager topologyManager) {
        this.topologyManager = topologyManager;
        topologyManager.addTopologyListener(new TopologyListener() {
            @Override
            public void nodeUpdated(final TopologyEvent event) {
                refresh();
            }

            @Override
            public void nodeRemoved(final TopologyEvent event) {
                refresh();
            }

            @Override
            public void nodeAdded(final TopologyEvent event) {
                refresh();
            }

            @Override
            public void driverUpdated(final TopologyEvent event) {
                refresh();
            }

            @Override
            public void driverRemoved(final TopologyEvent event) {
                refresh();
            }

            @Override
            public void driverAdded(final TopologyEvent event) {
                refresh();
            }
        });
        refresh();
        //don't count the first refresh
        lastRefresh = FDate.MIN_DATE;
    }

    public synchronized void maybeRefresh() {
        if (new Duration(lastRefresh).isGreaterThan(Duration.ONE_MINUTE)) {
            refresh();
        }
    }

    public synchronized void refresh() {
        final Triple<Integer, Integer, Integer> processingThreadsAndNodesAndDrivers = countProcessingThreads();
        processingThreadsCount = processingThreadsAndNodesAndDrivers.getFirst();
        nodesCount = processingThreadsAndNodesAndDrivers.getSecond();
        driversCount = processingThreadsAndNodesAndDrivers.getThird();
        lastRefresh = new FDate();
    }

    private Triple<Integer, Integer, Integer> countProcessingThreads() {
        final AtomicInteger processingThreads = new AtomicInteger(0);
        final AtomicInteger nodes = new AtomicInteger(0);
        final AtomicInteger drivers = new AtomicInteger(0);
        if (JPPFClientProperties.LOCAL_EXECUTION_ENABLED) {
            nodes.incrementAndGet();
            processingThreads.addAndGet(JPPFClientProperties.LOCAL_EXECUTION_THREADS);
        }
        new ATopologyVisitor() {
            @Override
            protected void visitNode(final TopologyNode node) {
                final JPPFSystemInformation systemInfo = TopologyNodes.extractSystemInfo(node);
                if (systemInfo != null) {
                    processingThreads.addAndGet(extractProcessingThreads(systemInfo));
                    nodes.incrementAndGet();
                }
            }

            @Override
            protected void visitDriver(final TopologyDriver driver) {
                drivers.incrementAndGet();
            }
        }.process(topologyManager);
        return Triple.of(processingThreads.get(), nodes.get(), drivers.get());
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

    public int getDriversCount() {
        return driversCount;
    }

    public int getNodesCount() {
        return nodesCount;
    }

    public TopologyManager getTopologyManager() {
        return topologyManager;
    }

}
