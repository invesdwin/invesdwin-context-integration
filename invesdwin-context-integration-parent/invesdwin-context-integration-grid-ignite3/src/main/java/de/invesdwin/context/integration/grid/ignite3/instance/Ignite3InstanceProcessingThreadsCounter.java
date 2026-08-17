package de.invesdwin.context.integration.grid.ignite3.instance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.Ignite;
import org.apache.ignite.network.ClusterNode;

import de.invesdwin.context.integration.grid.ignite3.AIgnite3ProcessingThreadsCounter;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedScheduledExecutorService;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public class Ignite3InstanceProcessingThreadsCounter extends AIgnite3ProcessingThreadsCounter {

    private final Ignite ignite;
    private final WrappedScheduledExecutorService executor;

    public Ignite3InstanceProcessingThreadsCounter(final Ignite ignite) {
        super();
        this.ignite = ignite;
        this.executor = Executors
                .newScheduledThreadPool(Ignite3InstanceProcessingThreadsCounter.class.getSimpleName() + "_refresh", 1);

        this.executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                maybeRefresh();
            }
        }, 5, 5, TimeUnit.SECONDS);

        refresh();
        lastRefresh = FDates.MIN_DATE;
    }

    @Override
    protected Pair<List<Integer>, Map<String, String>> countProcessingThreads() {
        final Map<String, Integer> serverThreads = ILockCollectionFactory.getInstance(false).newMap();
        final Map<String, String> localServerInfos = ILockCollectionFactory.getInstance(false).newMap();

        for (final ClusterNode node : ignite.cluster().nodes()) {
            final String uuid = node.id().toString();
            final int threads = Executors.getCpuThreadPoolCount();
            serverThreads.put(uuid, threads);
            localServerInfos.put(uuid, uuid + ":" + threads);
        }

        processHeartbeats(serverThreads, localServerInfos, null, false);

        final List<Integer> processingThreads = new ArrayList<>(serverThreads.values());

        return Pair.of(processingThreads, localServerInfos);
    }

    public Ignite getIgnite() {
        return ignite;
    }
}