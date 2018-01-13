package de.invesdwin.context.integration.jppf.client;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.jppf.client.JPPFConnectionPool;
import org.jppf.client.event.ClientQueueEvent;
import org.jppf.client.event.ClientQueueListener;

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

    @Override
    public void jobAdded(final ClientQueueEvent event) {
        synchronized (ConnectionSizingClientQueueListener.class) {
            ConnectionSizingClientQueueListener.activeJobsCount = Integers.max(0,
                    ConnectionSizingClientQueueListener.activeJobsCount + 1, event.getQueueSize());
            final int newCount = Integers.max(1, ConnectionSizingClientQueueListener.activeJobsCount);
            // grow the connection pools by one

            while (newCount > event.getClient().getAllConnectionsCount()) {
                for (final JPPFConnectionPool pool : event.getClient().getConnectionPools()) {
                    pool.setSize(pool.getSize() + 1);
                }
            }
        }
    }

    @Override
    public void jobRemoved(final ClientQueueEvent event) {
        synchronized (ConnectionSizingClientQueueListener.class) {
            ConnectionSizingClientQueueListener.activeJobsCount = Integers.max(0,
                    ConnectionSizingClientQueueListener.activeJobsCount - 1, event.getQueueSize());
            final int newCount = Integers.max(1, ConnectionSizingClientQueueListener.activeJobsCount);
            SCHEDULER.schedule(new Runnable() {
                @Override
                public void run() {
                    // shrink the connection pools by one
                    while (newCount < event.getClient().getAllConnectionsCount()) {
                        for (final JPPFConnectionPool pool : event.getClient().getConnectionPools()) {
                            final int newPoolSize = Integers.max(1, pool.getSize() - 1);
                            pool.setSize(newPoolSize);
                        }
                    }
                }
            }, REAPER_DELAY.longValue(FTimeUnit.MILLISECONDS), FTimeUnit.MILLISECONDS.timeUnitValue());
        }
    }

}
