package de.invesdwin.integration.jppf.topology;

import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;

import org.jppf.client.monitoring.topology.TopologyNode;
import org.jppf.management.JMXNodeConnectionWrapper;
import org.jppf.management.JPPFManagementInfo;
import org.jppf.management.JPPFSystemInformation;

import de.invesdwin.context.ContextProperties;

@Immutable
public final class TopologyNodes {

    private TopologyNodes() {}

    public static JPPFSystemInformation extractSystemInfo(final TopologyNode node) {
        final JMXNodeConnectionWrapper jmx = connect(node);
        try {
            final JPPFSystemInformation systemInfo = jmx.systemInformation();
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
        String host = managementInfo.getHost();
        if ("localhost".equals(host)) {
            host = node.getDriver().getManagementInfo().getHost();
        }
        final int port = managementInfo.getPort();
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
