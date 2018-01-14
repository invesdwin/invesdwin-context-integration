package de.invesdwin.context.integration.jppf.client;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
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

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.ftp.FtpFileChannel;
import de.invesdwin.context.integration.ftp.FtpServerDestinationProvider;
import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.jppf.topology.ATopologyVisitor;
import de.invesdwin.context.integration.jppf.topology.TopologyNodes;
import de.invesdwin.util.bean.tuple.Triple;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FTimeUnit;
import it.sauronsoftware.ftp4j.FTPFile;

@ThreadSafe
public class JPPFProcessingThreadsCounter {

    public static final Duration REFRESH_DURATION = Duration.ONE_MINUTE;

    public static final String FTP_DIRECTORY = JPPFProcessingThreadsCounter.class.getSimpleName();
    public static final String FTP_CONTENT_SEPARATOR = ";";
    public static final String FTP_CONTENT_DATEFORMAT = FDate.FORMAT_ISO_DATE_TIME_MS;
    public static final Duration HEARTBEAT_TIMEOUT = new Duration(3, FTimeUnit.MINUTES);

    private final TopologyManager topologyManager;
    private final FtpServerDestinationProvider ftpServerDestinationProvider;
    @GuardedBy("this")
    private int processingThreadsCount;
    @GuardedBy("this")
    private int driversCount;
    @GuardedBy("this")
    private int nodesCount;
    @GuardedBy("this")
    private FDate lastRefresh = FDate.MIN_DATE;

    private final WrappedExecutorService executor;

    public JPPFProcessingThreadsCounter(final TopologyManager topologyManager) {
        this.topologyManager = topologyManager;
        this.ftpServerDestinationProvider = MergedContext.getInstance().getBean(FtpServerDestinationProvider.class);
        this.executor = Executors.newFixedThreadPool(JPPFProcessingThreadsCounter.class.getSimpleName() + "_refresh",
                1);
        topologyManager.addTopologyListener(new TopologyListener() {
            @Override
            public void nodeUpdated(final TopologyEvent event) {
                refreshAsync();
            }

            @Override
            public void nodeRemoved(final TopologyEvent event) {
                refreshAsync();
            }

            @Override
            public void nodeAdded(final TopologyEvent event) {
                refreshAsync();
            }

            @Override
            public void driverUpdated(final TopologyEvent event) {
                refreshAsync();
            }

            @Override
            public void driverRemoved(final TopologyEvent event) {
                refreshAsync();
            }

            @Override
            public void driverAdded(final TopologyEvent event) {
                refreshAsync();
            }
        });
        refresh();
        //don't count the first refresh
        lastRefresh = FDate.MIN_DATE;
    }

    public synchronized void maybeRefresh() {
        if (new Duration(lastRefresh).isGreaterThan(REFRESH_DURATION)) {
            refresh();
        }
    }

    private void refreshAsync() {
        if (executor.getPendingCount() == 0) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    maybeRefresh();
                }
            });
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
        final Set<String> nodeUuids = new HashSet<>();
        new ATopologyVisitor() {
            @Override
            protected void visitNode(final TopologyNode node) {
                if (nodeUuids.add(node.getUuid())) {
                    final JPPFSystemInformation systemInfo = TopologyNodes.extractSystemInfo(node);
                    if (systemInfo != null) {
                        processingThreads.addAndGet(extractProcessingThreads(systemInfo));
                        nodes.incrementAndGet();
                    }
                }
            }

            @Override
            protected void visitDriver(final TopologyDriver driver) {
                drivers.incrementAndGet();
            }
        }.process(topologyManager);
        for (final URI ftpServerUri : ftpServerDestinationProvider.getDestinations()) {
            try (FtpFileChannel channel = new FtpFileChannel(ftpServerUri, FTP_DIRECTORY)) {
                channel.connect();
                for (final FTPFile file : channel.listFiles()) {
                    channel.setFilename(file.getName());
                    final FDate modified = FDate.valueOf(file.getModifiedDate());
                    if (new Duration(modified).isGreaterThan(HEARTBEAT_TIMEOUT)) {
                        channel.delete();
                        continue;
                    }
                    final byte[] content = channel.read();
                    if (content != null && content.length > 0) {
                        final String contentStr = new String(content);
                        final String[] split = Strings.split(contentStr, FTP_CONTENT_SEPARATOR);
                        if (split.length == 3) {
                            final String nodeUuid = split[0];
                            final Integer processingThreadsCount = Integer.valueOf(split[1]);
                            final FDate heartbeat = FDate.valueOf(split[2], FTP_CONTENT_DATEFORMAT);
                            if (new Duration(heartbeat).isGreaterThan(HEARTBEAT_TIMEOUT)) {
                                channel.delete();
                                continue;
                            }
                            if (nodeUuids.add(nodeUuid)) {
                                nodes.incrementAndGet();
                                processingThreads.addAndGet(processingThreadsCount);
                            }
                        }
                    }
                }
            }
        }
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
        maybeRefresh();
        return processingThreadsCount;
    }

    public synchronized int getDriversCount() {
        maybeRefresh();
        return driversCount;
    }

    public synchronized int getNodesCount() {
        maybeRefresh();
        return nodesCount;
    }

    public TopologyManager getTopologyManager() {
        return topologyManager;
    }

}
