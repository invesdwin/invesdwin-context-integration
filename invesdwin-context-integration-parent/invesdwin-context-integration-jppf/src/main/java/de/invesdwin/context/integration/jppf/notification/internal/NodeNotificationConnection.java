package de.invesdwin.context.integration.jppf.notification.internal;

import java.io.Closeable;

import javax.annotation.concurrent.ThreadSafe;
import javax.management.NotificationListener;

import org.jppf.client.monitoring.jobs.JobDispatch;
import org.jppf.management.JMXNodeConnectionWrapper;
import org.jppf.management.JPPFNodeTaskMonitorMBean;

import de.invesdwin.context.integration.jppf.topology.TopologyNodes;

@ThreadSafe
public class NodeNotificationConnection implements Closeable {

    private final String nodeUuid;
    private final JMXNodeConnectionWrapper jmx;
    private final JPPFNodeTaskMonitorMBean proxy;
    private final NotificationListener listener;

    public NodeNotificationConnection(final JobDispatch jobDispatch, final NotificationListener listener) {
        this.nodeUuid = jobDispatch.getNode().getUuid();
        this.listener = listener;
        this.jmx = TopologyNodes.connect(jobDispatch.getNode());
        try {
            this.proxy = jmx.getJPPFNodeTaskMonitorProxy();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        proxy.addNotificationListener(listener, null, null);
    }

    public String getNodeUuid() {
        return nodeUuid;
    }

    @Override
    public void close() {
        try {
            proxy.removeNotificationListener(listener);
            jmx.close();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}