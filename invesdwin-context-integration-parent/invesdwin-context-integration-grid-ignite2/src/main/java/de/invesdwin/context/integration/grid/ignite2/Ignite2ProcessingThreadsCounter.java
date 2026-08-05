package de.invesdwin.context.integration.grid.ignite2;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;

import com.github.sardine.DavResource;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.webdav.WebdavFileChannel;
import de.invesdwin.context.integration.webdav.WebdavServerDestinationProvider;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.bean.tuple.Triple;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.factory.pool.map.ICloseableMap;
import de.invesdwin.util.collections.factory.pool.map.linked.PooledLinkedMap;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public class Ignite2ProcessingThreadsCounter {

    public static final Duration REFRESH_INTERVAL = Duration.ONE_MINUTE;

    public static final String WEBDAV_DIRECTORY = Ignite2ProcessingThreadsCounter.class.getSimpleName();
    public static final String WEBDAV_CONTENT_SEPARATOR = ";";
    public static final String WEBDAV_CONTENT_DATEFORMAT = FDate.FORMAT_ISO_DATE_TIME_PS;
    public static final Duration HEARTBEAT_TIMEOUT = new Duration(5, FTimeUnit.MINUTES);

    // Kept original prefix names for backwards compatibility with Webdav writers
    public static final String DRIVER_HEARTBEAT_FILE_PREFIX = "driver_";
    public static final String NODE_HEARTBEAT_FILE_PREFIX = "node_";
    private static final int MAX_COUNT_HISTORY = 60;

    private static final Log LOG = new Log(Ignite2ProcessingThreadsCounter.class);

    private final Ignite ignite;
    private final WebdavServerDestinationProvider webdavServerDestinationProvider;

    @GuardedBy("this")
    private Map<String, String> nodeInfos = Collections.emptyMap();
    @GuardedBy("this")
    private Map<String, String> serverInfos = Collections.emptyMap();
    @GuardedBy("this")
    private final List<Integer> serversCounts = new ArrayList<Integer>();
    @GuardedBy("this")
    private final List<Integer> nodesCounts = new ArrayList<Integer>();
    @GuardedBy("this")
    private final List<Integer> sumProcessingThreadsCounts = new ArrayList<>();
    @GuardedBy("this")
    private final List<Integer> medianProcessingThreadsCounts = new ArrayList<>();
    @GuardedBy("this")
    private FDate lastRefresh = FDates.MIN_DATE;
    @GuardedBy("this")
    private boolean warmupFinished = false;

    private final WrappedExecutorService executor;

    public Ignite2ProcessingThreadsCounter(final Ignite ignite) {
        this.ignite = ignite;
        this.webdavServerDestinationProvider = MergedContext.getInstance()
                .getBean(WebdavServerDestinationProvider.class);
        this.executor = Executors.newFixedThreadPool(Ignite2ProcessingThreadsCounter.class.getSimpleName() + "_refresh",
                1);

        // Listen to Ignite topology changes instead of JPPF
        ignite.events().localListen(new IgnitePredicate<Event>() {
            @Override
            public boolean apply(final Event event) {
                refreshAsync();
                return true; // continue listening
            }
        }, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED);

        refresh();
        //don't count the first refresh
        lastRefresh = FDates.MIN_DATE;
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
        lastRefresh = FDate.now(); //prevent recursion

        final int processingThreadsCountBefore = getSumProcessingThreadsCount();
        final int serversCountBefore = getServersCount();
        final int nodesCountBefore = getNodesCount();

        final Triple<List<Integer>, Map<String, String>, Map<String, String>> processingThreadsAndServersAndNodes = countProcessingThreads();

        sumProcessingThreadsCounts.add(Integers.sum(processingThreadsAndServersAndNodes.getFirst()));
        Lists.maybeTrimSizeStart(sumProcessingThreadsCounts, MAX_COUNT_HISTORY);
        medianProcessingThreadsCounts.add(Integers.median(processingThreadsAndServersAndNodes.getFirst()));
        Lists.maybeTrimSizeStart(medianProcessingThreadsCounts, MAX_COUNT_HISTORY);

        serverInfos = sortInfos(processingThreadsAndServersAndNodes.getSecond());
        nodeInfos = sortInfos(processingThreadsAndServersAndNodes.getThird());

        serversCounts.add(serverInfos.size());
        Lists.maybeTrimSizeStart(serversCounts, MAX_COUNT_HISTORY);
        nodesCounts.add(nodeInfos.size());
        Lists.maybeTrimSizeStart(nodesCounts, MAX_COUNT_HISTORY);

        if (warmupFinished) {
            if (processingThreadsCountBefore != getSumProcessingThreadsCount()
                    || serversCountBefore != getServersCount() || nodesCountBefore != getNodesCount()) {
                logDetectedCounts();
            }
        }

        lastRefresh = FDate.now();
    }

    private Map<String, String> sortInfos(final Map<String, String> infos) {
        final List<String> sortedUUIDs = new ArrayList<>(infos.keySet());
        Collections.sort(sortedUUIDs);
        final Map<String, String> sortedInfos = ILockCollectionFactory.getInstance(false).newLinkedMap();
        for (final String uuid : sortedUUIDs) {
            sortedInfos.put(uuid, infos.get(uuid));
        }
        return sortedInfos;
    }

    private Triple<List<Integer>, Map<String, String>, Map<String, String>> countProcessingThreads() {
        final List<Integer> processingThreads = new ArrayList<>();
        final Map<String, String> localServerInfos = ILockCollectionFactory.getInstance(false).newMap();
        final Map<String, String> localNodeInfos = ILockCollectionFactory.getInstance(false).newMap();

        // 1. Process Ignite Server Nodes
        for (final ClusterNode node : ignite.cluster().forServers().nodes()) {
            final String uuid = node.id().toString();
            final int threads = node.metrics().getTotalCpus(); // proxy for processing threads
            processingThreads.add(threads);
            localServerInfos.put(uuid, uuid + ":" + threads + (node.isLocal() ? ":local" : ""));
        }

        // 2. Process Ignite Thick Clients
        for (final ClusterNode node : ignite.cluster().forClients().nodes()) {
            final String uuid = node.id().toString();
            final int threads = node.metrics().getTotalCpus(); // proxy for processing threads
            processingThreads.add(threads); // Add thick client cores to the grid's total thread count
            // Keep them in the client map, but append their thread counts so they show up in logs
            localNodeInfos.put(uuid, uuid + ":" + threads + (node.isLocal() ? ":local" : ""));
        }

        if (localNodeInfos.size() > 0) {
            processHeartbeats(processingThreads, localServerInfos, localNodeInfos);
        }
        return Triple.of(processingThreads, localServerInfos, localNodeInfos);
    }

    private void processHeartbeats(final List<Integer> processingThreads, final Map<String, String> localServerInfos,
            final Map<String, String> localNodeInfos) {
        for (final URI ftpServerUri : webdavServerDestinationProvider.getDestinations()) {
            try (WebdavFileChannel channel = new WebdavFileChannel(ftpServerUri, WEBDAV_DIRECTORY)) {
                channel.connect();
                final List<DavResource> listFiles = channel.listFiles();
                if (listFiles != null && !listFiles.isEmpty()) {
                    try (ICloseableMap<String, HeartbeatInfo> hostname_heartbeatinfo = PooledLinkedMap.getInstance()) {
                        for (int i = 0; i < listFiles.size(); i++) {
                            final DavResource file = listFiles.get(i);
                            processHeartbeat(hostname_heartbeatinfo, channel, file);
                        }

                        for (final HeartbeatInfo heartbeatInfo : hostname_heartbeatinfo.values()) {
                            if (heartbeatInfo.isDriver()) {
                                if (!localNodeInfos.containsKey(heartbeatInfo.getUuid())) {
                                    localNodeInfos.put(heartbeatInfo.getUuid(), heartbeatInfo.getUuid() + ":offline");
                                }
                            } else if (heartbeatInfo.isNode()) {
                                if (!localServerInfos.containsKey(heartbeatInfo.getUuid())) {
                                    localServerInfos.put(heartbeatInfo.getUuid(), heartbeatInfo.getUuid() + ":"
                                            + heartbeatInfo.getProcessingThreadsCount() + ":offline");
                                    processingThreads.add(heartbeatInfo.getProcessingThreadsCount());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void processHeartbeat(final Map<String, HeartbeatInfo> hostname_heartbeatInfo,
            final WebdavFileChannel channel, final DavResource file) {
        channel.setFilename(file.getName());
        final byte[] content = channel.download();
        if (content != null && content.length > 0) {
            final String contentStr = new String(content);
            final String[] split = Strings.splitPreserveAllTokens(contentStr, WEBDAV_CONTENT_SEPARATOR);
            if (split.length == 4) {
                final String hostname = split[0];
                final String uuid = split[1];
                final Integer processingThreadsCount = Integer.valueOf(split[2]);
                final FDate heartbeat = FDate.valueOf(split[3], WEBDAV_CONTENT_DATEFORMAT);
                if (new Duration(heartbeat).isGreaterThan(HEARTBEAT_TIMEOUT)) {
                    channel.delete();
                    return;
                }
                final HeartbeatInfo existing = hostname_heartbeatInfo.get(hostname);
                if (existing == null || heartbeat.isAfterNotNullSafe(existing.getHeartbeat())) {
                    hostname_heartbeatInfo.put(hostname,
                            new HeartbeatInfo(hostname, uuid, processingThreadsCount, heartbeat, file.getName()));
                }
            }
        }
    }

    public synchronized int getSumProcessingThreadsCount() {
        maybeRefresh();
        return Integers.max(0, Integers.max(sumProcessingThreadsCounts));
    }

    public synchronized int getMedianProcessingThreadsCount() {
        maybeRefresh();
        return Integers.max(0, Integers.max(medianProcessingThreadsCounts));
    }

    public synchronized int getNodesCount() {
        maybeRefresh();
        return Integers.max(0, Integers.max(nodesCounts));
    }

    public synchronized int getServersCount() {
        maybeRefresh();
        return Integers.max(0, Integers.max(serversCounts));
    }

    public Ignite getIgnite() {
        return ignite;
    }

    public synchronized void logWarmupFinished() {
        warmupFinished = true;
        logDetectedCounts();
    }

    private void logDetectedCounts() {
        final StringBuilder message = new StringBuilder();
        message.append(Ignite2ProcessingThreadsCounter.class.getSimpleName());
        message.append(" detected ");
        message.append(nodeInfos.size());
        message.append(" (~").append(getNodesCount()).append(")");
        message.append(" nodes");
        if (nodeInfos.size() != 1) {
            message.append("s");
        }
        message.append(" for ");
        message.append(serverInfos.size());
        message.append(" (~").append(getServersCount()).append(")");
        message.append(" servers");
        if (serverInfos.size() != 1) {
            message.append("s");
        }
        message.append(" with ");
        final int lastSumProcessingThreadsCount = Integers.max(0,
                sumProcessingThreadsCounts.get(sumProcessingThreadsCounts.size() - 1));
        message.append(lastSumProcessingThreadsCount);
        message.append(" (~").append(getSumProcessingThreadsCount()).append(")");
        message.append(" processing thread");
        if (lastSumProcessingThreadsCount != 1) {
            message.append("s");
        }
        message.append(" and ");
        final int lastMedianProcessingThreadsCount = Integers.max(0,
                medianProcessingThreadsCounts.get(medianProcessingThreadsCounts.size() - 1));
        message.append(lastMedianProcessingThreadsCount);
        message.append(" (~").append(Integers.max(0, getMedianProcessingThreadsCount())).append(")");
        message.append(" median batch size");
        message.append(": ");
        if (!nodeInfos.isEmpty()) {
            message.append("\nNodes: ");
            for (final String driver : nodeInfos.values()) {
                message.append("\n    - ");
                message.append(driver);
            }
        }
        message.append("\nServers: ");
        for (final String node : serverInfos.values()) {
            message.append("\n    - ");
            message.append(node);
        }
        LOG.info("%s", message);
    }

    public void waitForMinimumCounts(final int minimumNodesCount, final int minimumServersCount, final Duration timeout)
            throws TimeoutException {
        final Instant start = new Instant();
        boolean firstRun;
        synchronized (this) {
            firstRun = !warmupFinished;
        }
        do {
            if ((timeout != null && start.isGreaterThan(timeout))) {
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
        } while ((getNodesCount() < minimumNodesCount || getServersCount() < minimumServersCount));
    }

    private static final class HeartbeatInfo {
        private final String hostname;
        private final String uuid;
        private final Integer processingThreadsCount;
        private final FDate heartbeat;
        private final String fileName;

        private HeartbeatInfo(final String hostname, final String uuid, final Integer processingThreadsCount,
                final FDate heartbeat, final String fileName) {
            this.hostname = hostname;
            this.uuid = uuid;
            this.processingThreadsCount = processingThreadsCount;
            this.heartbeat = heartbeat;
            this.fileName = fileName;
        }

        public String getHostname() {
            return hostname;
        }

        public String getUuid() {
            return uuid;
        }

        public Integer getProcessingThreadsCount() {
            return processingThreadsCount;
        }

        public FDate getHeartbeat() {
            return heartbeat;
        }

        public boolean isDriver() {
            return fileName.startsWith(DRIVER_HEARTBEAT_FILE_PREFIX);
        }

        public boolean isNode() {
            return fileName.startsWith(NODE_HEARTBEAT_FILE_PREFIX);
        }
    }

}