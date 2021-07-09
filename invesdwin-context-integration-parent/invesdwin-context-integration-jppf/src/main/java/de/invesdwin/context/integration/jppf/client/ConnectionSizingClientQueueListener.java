package de.invesdwin.context.integration.jppf.client;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.jppf.client.JPPFClient;
import org.jppf.client.JPPFConnectionPool;
import org.jppf.client.event.ClientQueueEvent;
import org.jppf.client.event.ClientQueueListener;
import org.jppf.client.monitoring.jobs.JobMonitor;
import org.jppf.client.monitoring.jobs.JobMonitoringEvent;
import org.jppf.client.monitoring.jobs.JobMonitoringListenerAdapter;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedScheduledExecutorService;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

/**
 * http://www.jppf.org/doc/5.2/index.php?title=Notifications_of_client_job_queue_events
 */
@ThreadSafe
public class ConnectionSizingClientQueueListener implements ClientQueueListener {

    private static final WrappedScheduledExecutorService SCHEDULER = Executors
            .newScheduledThreadPool(ConnectionSizingClientQueueListener.class.getSimpleName() + "_REAPER", 1);
    //reap connections only when they are not used after a while
    private static final Duration REAPER_DELAY = Duration.ONE_MINUTE;
    private static final double MAX_CONNECTIONS_PER_NODE_MULTIPLIER = 2;

    private static volatile int queuedJobsCount = 0;
    private static volatile int activeJobsCount = 0;

    private static final Log LOG = new Log(ConnectionSizingClientQueueListener.class);

    public ConnectionSizingClientQueueListener(final JobMonitor jobMonitor) {
        jobMonitor.addJobMonitoringListener(new JobMonitoringListenerAdapter() {

            @Override
            public void jobRemoved(final JobMonitoringEvent event) {
                activeJobsCount--;
            }

            @Override
            public void jobAdded(final JobMonitoringEvent event) {
                activeJobsCount++;
            }

        });
    }

    @Override
    public void jobAdded(final ClientQueueEvent event) {
        synchronized (ConnectionSizingClientQueueListener.class) {
            ConnectionSizingClientQueueListener.queuedJobsCount = Integers.max(0,
                    ConnectionSizingClientQueueListener.queuedJobsCount + 1, event.getQueueSize());
            maybeIncreaseConnectionsCount(event.getClient(),
                    ConfiguredJPPFClient.getProcessingThreadsCounter().getNodesCount());
        }
    }

    public static void maybeIncreaseConnectionsCount(final JPPFClient client, final int nodesCount) {
        final int newCount = determineNewConnectionsCount(nodesCount);
        final int connectionsBefore = client.getAllConnectionsCount();
        int curConnections = connectionsBefore;
        while (newCount > curConnections) {
            final List<JPPFConnectionPool> connectionPools = client.getConnectionPools();
            if (connectionPools.isEmpty()) {
                break;
            }
            for (final JPPFConnectionPool pool : connectionPools) {
                pool.setSize(pool.getSize() + 1);
            }
            final int newConnections = client.getAllConnectionsCount();
            if (newConnections == curConnections) {
                break;
            }
            curConnections = newConnections;
        }
        logConnectionsChanged(connectionsBefore, curConnections);
    }

    @Override
    public void jobRemoved(final ClientQueueEvent event) {
        synchronized (ConnectionSizingClientQueueListener.class) {
            ConnectionSizingClientQueueListener.queuedJobsCount = Integers.max(0,
                    ConnectionSizingClientQueueListener.queuedJobsCount - 1, event.getQueueSize());
            if (SCHEDULER.getPendingCount() == 0) {
                SCHEDULER.schedule(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (ConnectionSizingClientQueueListener.class) {
                            final int newCount = determineNewConnectionsCount(
                                    ConfiguredJPPFClient.getProcessingThreadsCounter().getNodesCount());
                            final int connectionsBefore = event.getClient().getAllConnectionsCount();
                            int curConnections = connectionsBefore;
                            while (newCount < curConnections) {
                                final List<JPPFConnectionPool> connectionPools = event.getClient().getConnectionPools();
                                if (connectionPools.isEmpty()) {
                                    break;
                                }
                                int connectionsToDrop = curConnections - newCount;
                                for (final JPPFConnectionPool pool : connectionPools) {
                                    final int curPoolSize = pool.getSize();
                                    final int newPoolSize = Integers.max(1, curPoolSize - 1);
                                    pool.setSize(newPoolSize);
                                    connectionsToDrop--;
                                    if (connectionsToDrop <= 0) {
                                        break;
                                    }
                                }
                                try {
                                    FTimeUnit.SECONDS.sleep(1);
                                } catch (final InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                final int newConnections = event.getClient().getAllConnectionsCount();
                                if (newConnections == curConnections) {
                                    break;
                                }
                                curConnections = newConnections;
                            }
                            logConnectionsChanged(connectionsBefore, curConnections);
                        }
                    }
                }, REAPER_DELAY.longValue(FTimeUnit.MILLISECONDS), FTimeUnit.MILLISECONDS.timeUnitValue());
            }
        }
    }

    private static int determineNewConnectionsCount(final int nodesCount) {
        final int jobsCount = ConnectionSizingClientQueueListener.queuedJobsCount
                + ConnectionSizingClientQueueListener.activeJobsCount;
        final int nodesCountMultiplied = (int) (nodesCount * MAX_CONNECTIONS_PER_NODE_MULTIPLIER);
        return Integers.max(1, Math.min(jobsCount, nodesCountMultiplied), nodesCount);
    }

    private static void logConnectionsChanged(final int connectionsBefore, final int connectionsAfter) {
        if (connectionsBefore != connectionsAfter) {
            LOG.info("Modified JPPF connections from %s to %s", connectionsBefore, connectionsAfter);
        }
    }

}
