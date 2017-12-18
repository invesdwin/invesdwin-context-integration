package de.invesdwin.integration.jppf.notification;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.jppf.client.event.JobEvent;
import org.jppf.client.monitoring.jobs.JobMonitor;
import org.jppf.client.monitoring.jobs.JobMonitoringEvent;
import org.jppf.client.monitoring.jobs.JobMonitoringListener;

import de.invesdwin.integration.jppf.client.ConfiguredJPPFExecutorService;
import de.invesdwin.integration.jppf.notification.internal.BroadcastingNotificationListener;
import de.invesdwin.integration.jppf.notification.internal.NodeNotificationListeners;
import de.invesdwin.util.assertions.Assertions;

@ThreadSafe
public class TaskNotificationJPPFExecutorService extends ConfiguredJPPFExecutorService
        implements JobMonitoringListener, Closeable {

    private final JobMonitor jobMonitor;
    private final Map<String, NodeNotificationListeners> jobUuid_listeners = Collections
            .synchronizedMap(new HashMap<String, NodeNotificationListeners>());
    private final BroadcastingNotificationListener broadcastingNotificationListener = new BroadcastingNotificationListener();

    public TaskNotificationJPPFExecutorService(final JobMonitor jobMonitor) {
        super(jobMonitor.getTopologyManager().getJPPFClient());
        this.jobMonitor = jobMonitor;
        jobMonitor.addJobMonitoringListener(this);
    }

    @Override
    public final void jobStarted(final JobEvent event) {
        super.jobStarted(event);
        Assertions.checkNull(jobUuid_listeners.put(event.getJob().getUuid(),
                new NodeNotificationListeners(event.getJob().getUuid(), broadcastingNotificationListener)));
    }

    @Override
    public final void jobEnded(final JobEvent event) {
        super.jobEnded(event);
        Assertions.checkNotNull(jobUuid_listeners.remove(event.getJob().getUuid()));
    }

    @Override
    public final void driverAdded(final JobMonitoringEvent event) {
        //noop
    }

    @Override
    public final void driverRemoved(final JobMonitoringEvent event) {
        //noop
    }

    @Override
    public final void jobAdded(final JobMonitoringEvent event) {
        //noop
    }

    @Override
    public final void jobRemoved(final JobMonitoringEvent event) {
        //noop
    }

    @Override
    public final void jobUpdated(final JobMonitoringEvent event) {
        //noop
    }

    @Override
    public final void jobDispatchAdded(final JobMonitoringEvent event) {
        final NodeNotificationListeners listeners = jobUuid_listeners.get(event.getJob().getUuid());
        if (listeners != null) {
            listeners.add(event.getJobDispatch());
        }
    }

    @Override
    public final void jobDispatchRemoved(final JobMonitoringEvent event) {
        final NodeNotificationListeners listeners = jobUuid_listeners.get(event.getJob().getUuid());
        if (listeners != null) {
            listeners.remove(event.getJobDispatch());
        }
    }

    @Override
    public void close() {
        if (!isShutdown()) {
            shutdownNow();
        }
        jobMonitor.removeJobMonitoringListener(this);
        for (final NodeNotificationListeners listeners : jobUuid_listeners.values()) {
            listeners.close();
        }
        jobUuid_listeners.clear();
        broadcastingNotificationListener.clear();
    }

    public boolean registerTaskNotificationListener(final ATaskNotificationListener l) {
        return broadcastingNotificationListener.registerNotificationListener(l);
    }

    public boolean unregisterTaskNotificationListener(final ATaskNotificationListener l) {
        return broadcastingNotificationListener.unregisterNotificationListener(l);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }

}
