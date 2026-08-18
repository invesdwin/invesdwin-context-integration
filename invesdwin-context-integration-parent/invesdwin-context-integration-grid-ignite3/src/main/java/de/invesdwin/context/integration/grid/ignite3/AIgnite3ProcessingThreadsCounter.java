package de.invesdwin.context.integration.grid.ignite3;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.info.IFileInfo;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.context.integration.webdav.WebdavServerDestinationProvider;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.factory.pool.map.ICloseableMap;
import de.invesdwin.util.collections.factory.pool.map.linked.PooledLinkedMap;
import de.invesdwin.util.collections.list.Lists;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public abstract class AIgnite3ProcessingThreadsCounter {

    public static final Duration REFRESH_INTERVAL = Duration.ONE_MINUTE;

    public static final String WEBDAV_DIRECTORY = "Ignite2InstanceProcessingThreadsCounter";
    public static final String WEBDAV_CONTENT_SEPARATOR = ";";
    public static final String WEBDAV_CONTENT_DATEFORMAT = FDate.FORMAT_ISO_DATE_TIME_PS;
    public static final Duration HEARTBEAT_TIMEOUT = new Duration(5, FTimeUnit.MINUTES);

    public static final String NODE_HEARTBEAT_FILE_PREFIX = "node_";
    protected static final int MAX_COUNT_HISTORY = 60;

    private static final Log LOG = new Log(AIgnite3ProcessingThreadsCounter.class);

    protected final WebdavServerDestinationProvider webdavServerDestinationProvider;

    @GuardedBy("this")
    protected Map<String, String> serverInfos = Collections.emptyMap();
    @GuardedBy("this")
    protected final List<Integer> serversCounts = new ArrayList<Integer>();
    @GuardedBy("this")
    protected final List<Integer> sumProcessingThreadsCounts = new ArrayList<>();
    @GuardedBy("this")
    protected final List<Integer> medianProcessingThreadsCounts = new ArrayList<>();
    @GuardedBy("this")
    protected FDate lastRefresh = FDates.MIN_DATE;
    @GuardedBy("this")
    protected boolean warmupFinished = false;

    public AIgnite3ProcessingThreadsCounter() {
        this.webdavServerDestinationProvider = MergedContext.getInstance()
                .getBean(WebdavServerDestinationProvider.class);
    }

    public synchronized void maybeRefresh() {
        if (new Duration(lastRefresh).isGreaterThan(REFRESH_INTERVAL)) {
            refresh();
        }
    }

    public synchronized void refresh() {
        lastRefresh = FDate.now();

        final int processingThreadsCountBefore = getSumProcessingThreadsCount();
        final int serversCountBefore = getServersCount();

        // Returning a Pair directly mapping Ignite 3 unified nodes[cite: 48, 52]
        final Pair<List<Integer>, Map<String, String>> processingThreadsAndServers = countProcessingThreads();

        sumProcessingThreadsCounts.add(Integers.sum(processingThreadsAndServers.getFirst()));
        Lists.maybeTrimSizeStart(sumProcessingThreadsCounts, MAX_COUNT_HISTORY);
        medianProcessingThreadsCounts.add(Integers.median(processingThreadsAndServers.getFirst()));
        Lists.maybeTrimSizeStart(medianProcessingThreadsCounts, MAX_COUNT_HISTORY);

        serverInfos = sortInfos(processingThreadsAndServers.getSecond());

        serversCounts.add(serverInfos.size());
        Lists.maybeTrimSizeStart(serversCounts, MAX_COUNT_HISTORY);

        if (warmupFinished) {
            if (processingThreadsCountBefore != getSumProcessingThreadsCount()
                    || serversCountBefore != getServersCount()) {
                logDetectedCounts();
            }
        }

        lastRefresh = FDate.now();
    }

    protected abstract Pair<List<Integer>, Map<String, String>> countProcessingThreads();

    protected Map<String, String> sortInfos(final Map<String, String> infos) {
        final List<String> sortedUUIDs = new ArrayList<>(infos.keySet());
        Collections.sort(sortedUUIDs);
        final Map<String, String> sortedInfos = ILockCollectionFactory.getInstance(false).newLinkedMap();
        for (final String uuid : sortedUUIDs) {
            sortedInfos.put(uuid, infos.get(uuid));
        }
        return sortedInfos;
    }

    protected void processHeartbeats(final Map<String, Integer> serverThreads,
            final Map<String, String> localServerInfos, final Set<String> onlineNodeUuids,
            final boolean checkOnlineStatus) {
        for (final URI webdavServerUri : webdavServerDestinationProvider.getDestinations()) {
            try (IFileChannel channel = FileChannelRegistry.newInstance(webdavServerUri)
                    .setSubDirectory(WEBDAV_DIRECTORY)) {
                channel.connect();
                final List<? extends IFileInfo> listFiles = channel.listFiles();
                if (listFiles != null && !listFiles.isEmpty()) {
                    try (ICloseableMap<String, HeartbeatInfo> hostname_heartbeatinfo = PooledLinkedMap.getInstance()) {
                        for (int i = 0; i < listFiles.size(); i++) {
                            final IFileInfo file = listFiles.get(i);
                            processHeartbeat(hostname_heartbeatinfo, channel, file);
                        }

                        for (final HeartbeatInfo heartbeatInfo : hostname_heartbeatinfo.values()) {
                            final boolean online = !checkOnlineStatus
                                    || onlineNodeUuids.contains(heartbeatInfo.getUuid());
                            final String suffix = online ? "" : ":offline";

                            if (heartbeatInfo.isNode()) {
                                localServerInfos.put(heartbeatInfo.getUuid(), heartbeatInfo.getUuid() + ":"
                                        + heartbeatInfo.getProcessingThreadsCount() + suffix);
                                serverThreads.put(heartbeatInfo.getUuid(), heartbeatInfo.getProcessingThreadsCount());
                            }
                        }
                    }
                }
            }
        }
    }

    private void processHeartbeat(final Map<String, HeartbeatInfo> hostname_heartbeatInfo, final IFileChannel channel,
            final IFileInfo file) {
        channel.setFilename(file.getFilename());
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
                            new HeartbeatInfo(hostname, uuid, processingThreadsCount, heartbeat, file.getFilename()));
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

    public synchronized int getServersCount() {
        maybeRefresh();
        return Integers.max(0, Integers.max(serversCounts));
    }

    public synchronized void logWarmupFinished() {
        warmupFinished = true;
        logDetectedCounts();
    }

    protected void logDetectedCounts() {
        final StringBuilder message = new StringBuilder();
        message.append(getClass().getSimpleName());
        message.append(" detected ");
        message.append(serverInfos.size());
        message.append(" (~").append(getServersCount()).append(")");
        message.append(" server");
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

        message.append("\nServers: ");
        for (final String server : serverInfos.values()) {
            message.append("\n    - ");
            message.append(server);
        }
        LOG.info("%s", message);
    }

    public void waitForMinimumCounts(final int minimumServersCount, final Duration timeout) throws TimeoutException {
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
        } while (getServersCount() < minimumServersCount);
    }

    protected static final class HeartbeatInfo {
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

        public boolean isNode() {
            return fileName.startsWith(NODE_HEARTBEAT_FILE_PREFIX);
        }
    }
}