package de.invesdwin.context.integration.grid.ignite2.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;

import de.invesdwin.context.integration.grid.ignite2.AIgnite2ProcessingThreadsCounter;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.bean.tuple.Triple;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.time.date.FDates;

@ThreadSafe
public class Ignite2ClientProcessingThreadsCounter extends AIgnite2ProcessingThreadsCounter {

    private static final Log LOG = new Log(Ignite2ClientProcessingThreadsCounter.class);

    private final IgniteClient igniteClient;

    public Ignite2ClientProcessingThreadsCounter(final IgniteClient igniteClient) {
        super();
        this.igniteClient = igniteClient;
        refresh();
        lastRefresh = FDates.MIN_DATE;
    }

    @Override
    protected Triple<List<Integer>, Map<String, String>, Map<String, String>> countProcessingThreads() {
        final Map<String, Integer> serverThreads = ILockCollectionFactory.getInstance(false).newMap();
        final Map<String, Integer> nodeThreads = ILockCollectionFactory.getInstance(false).newMap();
        final Map<String, String> localServerInfos = ILockCollectionFactory.getInstance(false).newMap();
        final Map<String, String> localNodeInfos = ILockCollectionFactory.getInstance(false).newMap();

        final Set<String> onlineNodeUuids = ILockCollectionFactory.getInstance(false).newLinkedSet();
        try {
            if (igniteClient != null && igniteClient.cluster() != null) {
                for (final ClusterNode node : igniteClient.cluster().nodes()) {
                    onlineNodeUuids.add(node.id().toString());
                }
            }
        } catch (final Throwable t) {
            LOG.warn("Failed to query thin client cluster nodes for online status check", t);
        }

        processHeartbeats(serverThreads, nodeThreads, localServerInfos, localNodeInfos, onlineNodeUuids, true);

        final List<Integer> processingThreads = new ArrayList<>(serverThreads.size() + nodeThreads.size());
        processingThreads.addAll(serverThreads.values());
        processingThreads.addAll(nodeThreads.values());

        return Triple.of(processingThreads, localServerInfos, localNodeInfos);
    }
}