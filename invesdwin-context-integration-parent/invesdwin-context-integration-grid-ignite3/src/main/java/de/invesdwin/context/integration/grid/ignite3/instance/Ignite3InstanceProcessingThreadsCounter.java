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
        final List<Integer> processingThreads = new ArrayList<>();
        final Map<String, String> localServerInfos = ILockCollectionFactory.getInstance(false).newMap();

        // Process Ignite 3 cluster nodes uniformly using UUIDs
        for (final ClusterNode node : ignite.cluster().nodes()) {
            final String uuid = node.id().toString();
            final int threads = Runtime.getRuntime().availableProcessors();
            processingThreads.add(threads);
            localServerInfos.put(uuid, uuid + ":" + threads);
        }

        if (!localServerInfos.isEmpty()) {
            processHeartbeats(processingThreads, localServerInfos, null, false);
        }

        return Pair.of(processingThreads, localServerInfos);
    }

    public Ignite getIgnite() {
        return ignite;
    }
}