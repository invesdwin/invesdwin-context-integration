package de.invesdwin.context.integration.grid.ignite2.instance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;

import de.invesdwin.context.integration.grid.ignite2.AIgnite2ProcessingThreadsCounter;
import de.invesdwin.util.bean.tuple.Triple;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public class Ignite2InstanceProcessingThreadsCounter extends AIgnite2ProcessingThreadsCounter {

    private final Ignite ignite;
    private final WrappedExecutorService executor;

    public Ignite2InstanceProcessingThreadsCounter(final Ignite ignite) {
        super();
        this.ignite = ignite;
        this.executor = Executors
                .newFixedThreadPool(Ignite2InstanceProcessingThreadsCounter.class.getSimpleName() + "_refresh", 1);

        ignite.events().localListen(new IgnitePredicate<Event>() {
            @Override
            public boolean apply(final Event event) {
                refreshAsync();
                return true;
            }
        }, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED);

        refresh();
        lastRefresh = FDates.MIN_DATE;
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

    @Override
    protected Triple<List<Integer>, Map<String, String>, Map<String, String>> countProcessingThreads() {
        final List<Integer> processingThreads = new ArrayList<>();
        final Map<String, String> localServerInfos = ILockCollectionFactory.getInstance(false).newMap();
        final Map<String, String> localNodeInfos = ILockCollectionFactory.getInstance(false).newMap();

        for (final ClusterNode node : ignite.cluster().forServers().nodes()) {
            final String uuid = node.id().toString();
            final int threads = node.metrics().getTotalCpus();
            processingThreads.add(threads);
            localServerInfos.put(uuid, uuid + ":" + threads + (node.isLocal() ? ":local" : ""));
        }

        for (final ClusterNode node : ignite.cluster().forClients().nodes()) {
            final String uuid = node.id().toString();
            final int threads = node.metrics().getTotalCpus();
            processingThreads.add(threads);
            localNodeInfos.put(uuid, uuid + ":" + threads + (node.isLocal() ? ":local" : ""));
        }

        if (localNodeInfos.size() > 0) {
            processHeartbeats(processingThreads, localServerInfos, localNodeInfos, null, false);
        }
        return Triple.of(processingThreads, localServerInfos, localNodeInfos);
    }

    public Ignite getIgnite() {
        return ignite;
    }
}