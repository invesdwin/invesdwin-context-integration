package de.invesdwin.context.integration.jppf.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;

import org.jppf.client.monitoring.topology.TopologyDriver;
import org.jppf.client.monitoring.topology.TopologyNode;
import org.jppf.management.AllNodesSelector;
import org.jppf.management.JMXDriverConnectionWrapper;
import org.jppf.management.JPPFManagementInfo;
import org.jppf.management.JPPFSystemInformation;
import org.jppf.management.NodeSelector;
import org.jppf.management.forwarding.JPPFNodeForwardingMBean;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.uri.URIs;

@Immutable
public final class TopologyDrivers {

    public static final String NODE_FORWARDING_HOST_PREFIX = "forwarding:";

    private TopologyDrivers() {
    }

    public static List<TopologyNode> discoverHiddenNodes(final TopologyDriver driver) {
        final JMXDriverConnectionWrapper driverJmx = connect(driver);

        if (driverJmx == null) {
            return Collections.emptyList();
        }

        try {
            final JPPFNodeForwardingMBean proxy = driverJmx.getNodeForwarder();

            // this selector selects all nodes attached to the driver
            final NodeSelector selector = new AllNodesSelector();

            // invoke the state() method on the remote 'JPPFNodeAdminMBean' node MBeans
            // note that the MBean name does not need to be stated explicitely
            final Map<String, Object> results = proxy.systemInformation(selector);

            // handling the results
            final List<TopologyNode> nodes = new ArrayList<>();
            for (final Map.Entry<String, Object> entry : results.entrySet()) {
                if (entry.getValue() instanceof Exception) {
                    final Exception exc = (Exception) entry.getValue();
                    throw exc;
                } else {
                    final JPPFSystemInformation systemInfo = (JPPFSystemInformation) entry.getValue();
                    final JPPFManagementInfo nodeInformation = new JPPFManagementInfo(
                            NODE_FORWARDING_HOST_PREFIX + driver.getManagementInfo().getHost(),
                            driver.getManagementInfo().getIpAddress(), driver.getManagementInfo().getPort(),
                            systemInfo.getUuid().getProperty("jppf.uuid"), JPPFManagementInfo.NODE,
                            JPPFClientProperties.CLIENT_SSL_ENABLED);
                    nodeInformation.setSystemInfo(systemInfo);
                    final TopologyNode node = new TopologyNode(nodeInformation);
                    nodes.add(node);
                }
            }
            return nodes;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                driverJmx.close();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static JPPFSystemInformation extractSystemInfo(final TopologyDriver driver) {
        final JPPFManagementInfo managementInfo = driver.getManagementInfo();
        if (managementInfo.getSystemInfo() != null) {
            return managementInfo.getSystemInfo();
        }
        final JMXDriverConnectionWrapper jmx = connect(driver);
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

    public static JMXDriverConnectionWrapper connect(final TopologyDriver driver) {
        final JPPFManagementInfo managementInfo = driver.getManagementInfo();
        final String host = managementInfo.getHost();
        final int port = managementInfo.getPort();
        if (!URIs.connect("p://" + host + ":" + port).isServerResponding()) {
            return null;
        }
        final boolean secure = managementInfo.isSecure();
        final JMXDriverConnectionWrapper jmx = new JMXDriverConnectionWrapper(host, port, secure);
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
