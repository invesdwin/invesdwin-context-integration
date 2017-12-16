package de.invesdwin.integration.jppf.notification.internal;

import java.io.Closeable;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.ThreadSafe;
import javax.management.NotificationListener;

import org.jppf.client.monitoring.jobs.JobDispatch;
import org.jppf.management.JMXNodeConnectionWrapper;
import org.jppf.management.JPPFManagementInfo;
import org.jppf.management.JPPFNodeTaskMonitorMBean;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.time.fdate.FTimeUnit;

@ThreadSafe
public class NodeNotificationConnection implements Closeable {

    private static final long CONNECT_TIMEOUT_MILLIS = ContextProperties.DEFAULT_NETWORK_TIMEOUT
            .longValue(FTimeUnit.MILLISECONDS);

    private final String nodeUuid;
    private final JMXNodeConnectionWrapper jmx;
    private JPPFNodeTaskMonitorMBean proxy;
    private final NotificationListener listener;

    public NodeNotificationConnection(final JobDispatch jobDispatch, final NotificationListener listener) {
        this.nodeUuid = jobDispatch.getNode().getUuid();
        this.listener = listener;
        final JPPFManagementInfo managementInfo = jobDispatch.getNode().getManagementInfo();
        final String host = managementInfo.getHost();
        final int port = managementInfo.getPort();
        final boolean secure = managementInfo.isSecure();
        this.jmx = new JMXNodeConnectionWrapper(host, port, secure);
        try {
            jmx.connectAndWait(CONNECT_TIMEOUT_MILLIS);
            if (!jmx.isConnected()) {
                throw new TimeoutException("JMX connect timeout [" + CONNECT_TIMEOUT_MILLIS + " ms] exceeded for: host="
                        + host + " port=" + port + " secure=" + secure);
            }
            this.proxy = jmx.getJPPFNodeTaskMonitorProxy();
            proxy.addNotificationListener(listener, null, null);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
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