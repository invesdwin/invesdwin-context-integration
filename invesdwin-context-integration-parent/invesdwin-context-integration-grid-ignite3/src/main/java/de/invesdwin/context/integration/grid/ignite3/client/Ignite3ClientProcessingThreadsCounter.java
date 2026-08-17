package de.invesdwin.context.integration.grid.ignite3.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.network.ClusterNode;

import de.invesdwin.context.integration.grid.ignite3.AIgnite3ProcessingThreadsCounter;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.bean.tuple.Pair;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public class Ignite3ClientProcessingThreadsCounter extends AIgnite3ProcessingThreadsCounter {

    private static final Log LOG = new Log(Ignite3ClientProcessingThreadsCounter.class);

    private final IgniteClient igniteClient;

    public Ignite3ClientProcessingThreadsCounter(final IgniteClient igniteClient) {
        super();
        this.igniteClient = igniteClient;
        refresh();
        lastRefresh = FDates.MIN_DATE;
    }

    @Override
    protected Pair<List<Integer>, Map<String, String>> countProcessingThreads() {
        final Map<String, Integer> serverThreads = ILockCollectionFactory.getInstance(false).newMap();
        final Map<String, String> localServerInfos = ILockCollectionFactory.getInstance(false).newMap();

        final Set<String> onlineNodeUuids = ILockCollectionFactory.getInstance(false).newLinkedSet();
        try {
            if (igniteClient != null) {
                for (final ClusterNode node : igniteClient.cluster().nodes()) {
                    onlineNodeUuids.add(node.id().toString());
                }
            }
        } catch (final Throwable t) {
            LOG.warn("Failed to query thin client cluster nodes for online status check", t);
        }

        processHeartbeats(serverThreads, localServerInfos, onlineNodeUuids, true);

        final List<Integer> processingThreads = new ArrayList<>(serverThreads.values());

        return Pair.of(processingThreads, localServerInfos);
    }
}