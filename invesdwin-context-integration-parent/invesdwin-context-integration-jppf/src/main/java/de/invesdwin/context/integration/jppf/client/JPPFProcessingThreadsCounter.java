package de.invesdwin.context.integration.jppf.client;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

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

import com.github.sardine.DavResource;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.jppf.topology.ATopologyVisitor;
import de.invesdwin.context.integration.jppf.topology.TopologyNodes;
import de.invesdwin.context.integration.webdav.WebdavFileChannel;
import de.invesdwin.context.integration.webdav.WebdavServerDestinationProvider;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.bean.tuple.Triple;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FTimeUnit;

@ThreadSafe
public class JPPFProcessingThreadsCounter {

    public static final Duration REFRESH_INTERVAL = Duration.ONE_MINUTE;

    public static final String WEBDAV_DIRECTORY = JPPFProcessingThreadsCounter.class.getSimpleName();
    public static final String WEBDAV_CONTENT_SEPARATOR = ";";
    public static final String WEBDAV_CONTENT_DATEFORMAT = FDate.FORMAT_ISO_DATE_TIME_MS;
    public static final Duration HEARTBEAT_TIMEOUT = new Duration(5, FTimeUnit.MINUTES);
    public static final String DRIVER_HEARTBEAT_FILE_PREFIX = "driver_";
    public static final String NODE_HEARTBEAT_FILE_PREFIX = "node_";
    private static final int MAX_COUNT_HISTORY = 60;

    private static final Log LOG = new Log(JPPFProcessingThreadsCounter.class);

    private final TopologyManager topologyManager;
    private final WebdavServerDestinationProvider webdavServerDestinationProvider;
    @GuardedBy("this")
    private Map<String, String> driverInfos = Collections.emptyMap();
    @GuardedBy("this")
    private Map<String, String> nodeInfos = Collections.emptyMap();
    @GuardedBy("this")
    private final List<Integer> nodesCounts = new ArrayList<Integer>();
    @GuardedBy("this")
    private final List<Integer> driversCounts = new ArrayList<Integer>();
    @GuardedBy("this")
    private final List<Integer> sumProcessingThreadsCounts = new ArrayList<>();
    @GuardedBy("this")
    private final List<Integer> medianProcessingThreadsCounts = new ArrayList<>();
    @GuardedBy("this")
    private FDate lastRefresh = FDate.MIN_DATE;
    @GuardedBy("this")
    private boolean warmupFinished = false;

    private final WrappedExecutorService executor;

    public JPPFProcessingThreadsCounter(final TopologyManager topologyManager) {
        this.topologyManager = topologyManager;
        this.webdavServerDestinationProvider = MergedContext.getInstance()
                .getBean(WebdavServerDestinationProvider.class);
        this.executor = Executors.newFixedThreadPool(JPPFProcessingThreadsCounter.class.getSimpleName() + "_refresh",
                1);
        topologyManager.addTopologyListener(new TopologyListener() {
            @Override
            public void nodeUpdated(final TopologyEvent event) {}

            @Override
            public void nodeRemoved(final TopologyEvent event) {
                refreshAsync();
            }

            @Override
            public void nodeAdded(final TopologyEvent event) {
                refreshAsync();
            }

            @Override
            public void driverUpdated(final TopologyEvent event) {}

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
        if (new Duration(lastRefresh).isGreaterThan(REFRESH_INTERVAL)) {
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
        lastRefresh = new FDate(); //prevent recursion

        final int processingThreadsCountBefore = getSumProcessingThreadsCount();
        final int nodesCountBefore = getNodesCount();
        final int driversCountBefore = getDriversCount();

        final Triple<List<Integer>, Map<String, String>, Map<String, String>> processingThreadsAndNodesAndDrivers = countProcessingThreads();
        sumProcessingThreadsCounts.add(Integers.sum(processingThreadsAndNodesAndDrivers.getFirst()));
        medianProcessingThreadsCounts.add(Integers.median(processingThreadsAndNodesAndDrivers.getFirst()));
        while (sumProcessingThreadsCounts.size() > MAX_COUNT_HISTORY) {
            sumProcessingThreadsCounts.remove(0);
        }
        while (medianProcessingThreadsCounts.size() > MAX_COUNT_HISTORY) {
            medianProcessingThreadsCounts.remove(0);
        }
        nodeInfos = sortInfos(processingThreadsAndNodesAndDrivers.getSecond());
        driverInfos = sortInfos(processingThreadsAndNodesAndDrivers.getThird());
        nodesCounts.add(nodeInfos.size());
        while (nodesCounts.size() > MAX_COUNT_HISTORY) {
            nodesCounts.remove(0);
        }
        driversCounts.add(driverInfos.size());
        while (driversCounts.size() > MAX_COUNT_HISTORY) {
            driversCounts.remove(0);
        }

        if (warmupFinished) {
            if (processingThreadsCountBefore != getSumProcessingThreadsCount() || nodesCountBefore != getNodesCount()
                    || driversCountBefore != getDriversCount()) {
                logDetectedCounts();
            }
        }

        ConnectionSizingClientQueueListener.maybeIncreaseConnectionsCount(topologyManager.getJPPFClient(),
                getNodesCount());

        lastRefresh = new FDate();
    }

    private Map<String, String> sortInfos(final Map<String, String> infos) {
        final List<String> sortedUUIDs = new ArrayList<>(infos.keySet());
        Collections.sort(sortedUUIDs);
        final Map<String, String> sortedInfos = new LinkedHashMap<>();
        for (final String uuid : sortedUUIDs) {
            sortedInfos.put(uuid, infos.get(uuid));
        }
        return sortedInfos;
    }

    private Triple<List<Integer>, Map<String, String>, Map<String, String>> countProcessingThreads() {
        final List<Integer> processingThreads = new ArrayList<>();
        final Map<String, String> nodeInfos = new HashMap<String, String>();
        final Map<String, String> driverInfos = new HashMap<String, String>();
        if (JPPFClientProperties.LOCAL_EXECUTION_ENABLED) {
            final int threads = JPPFClientProperties.LOCAL_EXECUTION_THREADS;
            final String uuid = topologyManager.getJPPFClient().getUuid();
            nodeInfos.put(uuid, uuid + ":" + threads + ":local");
            processingThreads.add(threads);
        }
        new ATopologyVisitor() {
            @Override
            protected void visitNode(final TopologyNode node) {
                final JPPFSystemInformation systemInfo = TopologyNodes.extractSystemInfo(node);
                if (systemInfo != null) {
                    final String uuid = node.getUuid();
                    if (!nodeInfos.containsKey(uuid)) {
                        final Integer threads = extractProcessingThreads(systemInfo);
                        processingThreads.add(threads);
                        nodeInfos.put(uuid, uuid + ":" + threads);
                    }
                }
            }

            @Override
            protected void visitDriver(final TopologyDriver driver) {
                final String uuid = driver.getUuid();
                if (!driverInfos.containsKey(uuid)) {
                    driverInfos.put(uuid, uuid);
                }
            }
        }.process(topologyManager);
        for (final URI ftpServerUri : webdavServerDestinationProvider.getDestinations()) {
            try (WebdavFileChannel channel = new WebdavFileChannel(ftpServerUri, WEBDAV_DIRECTORY)) {
                channel.connect();
                for (final DavResource file : channel.listFiles()) {
                    processHeartbeat(processingThreads, nodeInfos, driverInfos, channel, file);
                }
            }
        }
        return Triple.of(processingThreads, nodeInfos, driverInfos);
    }

    private void processHeartbeat(final List<Integer> processingThreads, final Map<String, String> nodeInfos,
            final Map<String, String> driverInfos, final WebdavFileChannel channel, final DavResource file) {
        channel.setFilename(file.getName());
        final byte[] content = channel.download();
        if (content != null && content.length > 0) {
            final String contentStr = new String(content);
            final String[] split = Strings.split(contentStr, WEBDAV_CONTENT_SEPARATOR);
            if (split.length == 3) {
                final String uuid = split[0];
                final Integer processingThreadsCount = Integer.valueOf(split[1]);
                final FDate heartbeat = FDate.valueOf(split[2], WEBDAV_CONTENT_DATEFORMAT);
                if (new Duration(heartbeat).isGreaterThan(HEARTBEAT_TIMEOUT)) {
                    channel.delete();
                    return;
                }
                if (file.getName().startsWith(DRIVER_HEARTBEAT_FILE_PREFIX)) {
                    if (!driverInfos.containsKey(uuid)) {
                        driverInfos.put(uuid, uuid + ":offline");
                    }
                } else if (file.getName().startsWith(NODE_HEARTBEAT_FILE_PREFIX)) {
                    if (!nodeInfos.containsKey(uuid)) {
                        nodeInfos.put(uuid, uuid + ":" + processingThreadsCount + ":offline");
                        processingThreads.add(processingThreadsCount);
                    }
                }
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

    public synchronized int getSumProcessingThreadsCount() {
        maybeRefresh();
        return Integers.max(sumProcessingThreadsCounts);
    }

    public synchronized int getMedianProcessingThreadsCount() {
        maybeRefresh();
        return Integers.max(medianProcessingThreadsCounts);
    }

    public synchronized int getDriversCount() {
        maybeRefresh();
        return Integers.max(driversCounts);
    }

    public synchronized int getNodesCount() {
        maybeRefresh();
        return Integers.max(nodesCounts);
    }

    public TopologyManager getTopologyManager() {
        return topologyManager;
    }

    public synchronized void logWarmupFinished() {
        warmupFinished = true;
        logDetectedCounts();
    }

    private void logDetectedCounts() {
        final StringBuilder message = new StringBuilder();
        message.append(JPPFProcessingThreadsCounter.class.getSimpleName());
        message.append(" detected ");
        message.append(driverInfos.size());
        message.append(" (~").append(getDriversCount()).append(")");
        message.append(" driver");
        if (driverInfos.size() != 1) {
            message.append("s");
        }
        message.append(" for ");
        message.append(nodeInfos.size());
        message.append(" (~").append(getNodesCount()).append(")");
        message.append(" node");
        if (nodeInfos.size() != 1) {
            message.append("s");
        }
        message.append(" with ");
        final int lastSumProcessingThreadsCount = sumProcessingThreadsCounts.get(sumProcessingThreadsCounts.size() - 1);
        message.append(lastSumProcessingThreadsCount);
        message.append(" (~").append(getSumProcessingThreadsCount()).append(")");
        message.append(" processing thread");
        if (lastSumProcessingThreadsCount != 1) {
            message.append("s");
        }
        message.append(" and ");
        final int lastMedianProcessingThreadsCount = medianProcessingThreadsCounts
                .get(medianProcessingThreadsCounts.size() - 1);
        message.append(lastMedianProcessingThreadsCount);
        message.append(" (~").append(getMedianProcessingThreadsCount()).append(")");
        message.append(" median batch size");
        message.append(": ");
        if (!driverInfos.isEmpty()) {
            message.append("\nDrivers: ");
            for (final String driver : driverInfos.values()) {
                message.append("\n    - ");
                message.append(driver);
            }
        }
        message.append("\nNodes: ");
        for (final String node : nodeInfos.values()) {
            message.append("\n    - ");
            message.append(node);
        }
        LOG.info("%s", message);

        ConfiguredJPPFClient.getBatchedExecutorService().setBatchSize(getMedianProcessingThreadsCount());
    }

    public void waitForMinimumCounts(final int minimumDriversCount, final int minimumNodesCount, final Duration timeout)
            throws TimeoutException {
        final Instant start = new Instant();
        boolean firstRun = !warmupFinished;
        do {
            if ((timeout != null && start.toDuration().isGreaterThan(timeout))) {
                throw new TimeoutException("timeout exceeded: " + timeout);
            }
            try {
                FTimeUnit.SECONDS.sleep(1);
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (!firstRun) {
                refresh();
            }
            firstRun = false;
        } while ((getDriversCount() < minimumDriversCount || getNodesCount() < minimumNodesCount));
    }

}
