package de.invesdwin.context.integration.jppf.notification.internal;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;
import javax.management.NotificationListener;

import org.jppf.client.monitoring.jobs.JobDispatch;

import de.invesdwin.util.assertions.Assertions;

@ThreadSafe
public class NodeNotificationListeners implements Closeable {
    private final String jobUuid;
    private final Map<String, NodeNotificationConnection> nodeUuid_connection = Collections
            .synchronizedMap(new HashMap<>());
    private final NotificationListener listener;

    public NodeNotificationListeners(final String jobUuid, final NotificationListener listener) {
        this.jobUuid = jobUuid;
        this.listener = listener;
    }

    public void add(final JobDispatch jobDispatch) {
        Assertions.checkNull(nodeUuid_connection.put(jobDispatch.getNode().getUuid(),
                new NodeNotificationConnection(jobDispatch, listener)));
    }

    public void remove(final JobDispatch jobDispatch) {
        final NodeNotificationConnection connection = nodeUuid_connection.get(jobDispatch.getNode().getUuid());
        if (connection != null) {
            connection.close();
        }
    }

    public String getJobUuid() {
        return jobUuid;
    }

    @Override
    public void close() {
        for (final NodeNotificationConnection connection : nodeUuid_connection.values()) {
            connection.close();
        }
        nodeUuid_connection.clear();
    }

}