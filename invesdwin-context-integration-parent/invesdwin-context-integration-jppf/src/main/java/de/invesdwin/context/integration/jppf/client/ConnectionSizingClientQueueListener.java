package de.invesdwin.context.integration.jppf.client;

import java.util.List;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.jppf.client.JPPFConnectionPool;
import org.jppf.client.event.ClientQueueEvent;
import org.jppf.client.event.ClientQueueListener;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedScheduledExecutorService;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

/**
 * http://www.jppf.org/doc/5.2/index.php?title=Notifications_of_client_job_queue_events
 */
@ThreadSafe
public class ConnectionSizingClientQueueListener implements ClientQueueListener {

    private static final WrappedScheduledExecutorService SCHEDULER = Executors
            .newScheduledThreadPool(ConnectionSizingClientQueueListener.class.getSimpleName() + "_REAPER", 1);
    //reap connections only when they are not used after a while
    private static final Duration REAPER_DELAY = Duration.ONE_MINUTE;

    @GuardedBy("ConnectionSizingClientQueueListener.class")
    private static int activeJobsCount = 0;

    private final Log log = new Log(this);

    @Override
    public void jobAdded(final ClientQueueEvent event) {
        synchronized (ConnectionSizingClientQueueListener.class) {
            ConnectionSizingClientQueueListener.activeJobsCount = Integers.max(0,
                    ConnectionSizingClientQueueListener.activeJobsCount + 1, event.getQueueSize());
            final int newCount = determineNewConnectionsCount(ConnectionSizingClientQueueListener.activeJobsCount);
            // grow the connection pools by one

            final int connectionsBefore = event.getClient().getAllConnectionsCount();
            int curConnections = connectionsBefore;
            while (newCount > curConnections) {
                final List<JPPFConnectionPool> connectionPools = event.getClient().getConnectionPools();
                if (connectionPools.isEmpty()) {
                    break;
                }
                for (final JPPFConnectionPool pool : connectionPools) {
                    pool.setSize(pool.getSize() + 1);
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

    @Override
    public void jobRemoved(final ClientQueueEvent event) {
        synchronized (ConnectionSizingClientQueueListener.class) {
            ConnectionSizingClientQueueListener.activeJobsCount = Integers.max(0,
                    ConnectionSizingClientQueueListener.activeJobsCount - 1, event.getQueueSize());
            final int newCount = determineNewConnectionsCount(ConnectionSizingClientQueueListener.activeJobsCount);
            SCHEDULER.schedule(new Runnable() {
                @Override
                public void run() {
                    // shrink the connection pools by one
                    final int connectionsBefore = event.getClient().getAllConnectionsCount();
                    int curConnections = connectionsBefore;
                    while (newCount < curConnections) {
                        final List<JPPFConnectionPool> connectionPools = event.getClient().getConnectionPools();
                        if (connectionPools.isEmpty()) {
                            break;
                        }
                        for (final JPPFConnectionPool pool : connectionPools) {
                            final int newPoolSize = Integers.max(1, pool.getSize() - 1);
                            pool.setSize(newPoolSize);
                        }
                        final int newConnections = event.getClient().getAllConnectionsCount();
                        if (newConnections == curConnections) {
                            break;
                        }
                        curConnections = newConnections;
                    }
                    logConnectionsChanged(connectionsBefore, curConnections);
                }
            }, REAPER_DELAY.longValue(FTimeUnit.MILLISECONDS), FTimeUnit.MILLISECONDS.timeUnitValue());
        }
    }

    private int determineNewConnectionsCount(final int activeJobsCount) {
        final int nodesCount = ConfiguredJPPFClient.getProcessingThreadsCounter().getNodesCount();
        return Integers.max(1, activeJobsCount, nodesCount);
    }

    protected void logConnectionsChanged(final int connectionsBefore, final int connectionsAfter) {
        if (connectionsBefore != connectionsAfter) {
            log.info("Modified JPPF connections from %s to %s", connectionsBefore, connectionsAfter);
        }
    }

}
