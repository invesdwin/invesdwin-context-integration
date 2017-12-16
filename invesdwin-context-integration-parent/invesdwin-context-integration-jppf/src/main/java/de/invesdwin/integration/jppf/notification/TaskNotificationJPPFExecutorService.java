package de.invesdwin.integration.jppf.notification;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.jppf.client.concurrent.JPPFExecutorService;
import org.jppf.client.event.JobEvent;
import org.jppf.client.monitoring.jobs.JobDispatch;
import org.jppf.client.monitoring.jobs.JobMonitor;
import org.jppf.client.monitoring.jobs.JobMonitoringEvent;
import org.jppf.client.monitoring.jobs.JobMonitoringListener;
import org.jppf.management.JPPFManagementInfo;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.concurrent.AFastIterableDelegateSet;

@ThreadSafe
public class TaskNotificationJPPFExecutorService extends JPPFExecutorService
        implements JobMonitoringListener, Closeable {

    private final JobMonitor jobMonitor;
    private final Map<String, NodeNotificationListeners> jobUuid_listeners = Collections
            .synchronizedMap(new HashMap<String, NodeNotificationListeners>());
    private final AFastIterableDelegateSet<ATaskNotificationListener> taskNotificationListeners = new AFastIterableDelegateSet<ATaskNotificationListener>() {
        @Override
        protected Set<ATaskNotificationListener> newDelegate() {
            return new LinkedHashSet<ATaskNotificationListener>();
        }
    };

    public TaskNotificationJPPFExecutorService(final JobMonitor jobMonitor) {
        super(jobMonitor.getTopologyManager().getJPPFClient());
        this.jobMonitor = jobMonitor;
        jobMonitor.addJobMonitoringListener(this);
    }

    @Override
    public final void jobStarted(final JobEvent event) {
        super.jobStarted(event);
        Assertions.checkNull(jobUuid_listeners.put(event.getJob().getUuid(),
                new NodeNotificationListeners(event.getJob().getUuid())));
    }

    @Override
    public final void jobEnded(final JobEvent event) {
        super.jobEnded(event);
        Assertions.checkNotNull(jobUuid_listeners.remove(event.getJob().getUuid()));
    }

    @Override
    public final void driverAdded(final JobMonitoringEvent event) {}

    @Override
    public final void driverRemoved(final JobMonitoringEvent event) {}

    @Override
    public final void jobAdded(final JobMonitoringEvent event) {}

    @Override
    public final void jobRemoved(final JobMonitoringEvent event) {}

    @Override
    public final void jobUpdated(final JobMonitoringEvent event) {}

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
        taskNotificationListeners.clear();
    }

    public boolean registerTaskNotificationListener(final ATaskNotificationListener l) {
        return taskNotificationListeners.add(l);
    }

    public boolean unregisterTaskNotificationListener(final ATaskNotificationListener l) {
        return taskNotificationListeners.remove(l);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }

    private final class NodeNotificationListeners implements Closeable {
        private final String jobUuid;
        private final Map<String, Void> map = Collections.synchronizedMap(new HashMap<>());

        private NodeNotificationListeners(final String jobUuid) {
            this.jobUuid = jobUuid;
        }

        public void add(final JobDispatch jobDispatch) {
            final JPPFManagementInfo managementInfo = jobDispatch.getNode().getManagementInfo();
            final String host = managementInfo.getHost();
            final int port = managementInfo.getPort();
            final boolean secure = managementInfo.isSecure();
        }

        public void remove(final JobDispatch jobDispatch) {}

        @Override
        public void close() {}

    }

}
