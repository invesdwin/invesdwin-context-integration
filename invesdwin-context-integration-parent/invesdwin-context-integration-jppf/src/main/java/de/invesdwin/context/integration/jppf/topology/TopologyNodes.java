package de.invesdwin.context.integration.jppf.topology;

import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;

import org.jppf.client.monitoring.topology.TopologyNode;
import org.jppf.management.JMXNodeConnectionWrapper;
import org.jppf.management.JPPFManagementInfo;
import org.jppf.management.JPPFSystemInformation;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.lang.uri.URIs;

@Immutable
public final class TopologyNodes {

    private TopologyNodes() {}

    public static JPPFSystemInformation extractSystemInfo(final TopologyNode node) {
        final JPPFManagementInfo managementInfo = node.getManagementInfo();
        if (managementInfo.getSystemInfo() != null) {
            return managementInfo.getSystemInfo();
        }
        final JMXNodeConnectionWrapper jmx = connect(node);
        if (jmx == null) {
            return null;
        }
        try {
            final JPPFSystemInformation systemInfo = jmx.systemInformation();
            managementInfo.setSystemInfo(systemInfo); //cache the system info
            return systemInfo;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                jmx.close();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static JMXNodeConnectionWrapper connect(final TopologyNode node) {
        final JPPFManagementInfo managementInfo = node.getManagementInfo();
        final String host = managementInfo.getHost();
        //local nodes advertise the host wrong
        if (host.startsWith(TopologyDrivers.NODE_FORWARDING_HOST_PREFIX)) {
            throw new IllegalStateException("please use node forwarding requests");
        }
        final int port = managementInfo.getPort();
        if (!URIs.connect("p://" + host + ":" + port).isServerResponding()) {
            return null;
        }
        final boolean secure = managementInfo.isSecure();
        final JMXNodeConnectionWrapper jmx = new JMXNodeConnectionWrapper(host, port, secure);
        try {
            jmx.connectAndWait(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
            if (!jmx.isConnected()) {
                throw new TimeoutException("JMX connect timeout [" + ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS
                        + " ms] exceeded for: host=" + host + " port=" + port + " secure=" + secure);
            }
            return jmx;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
