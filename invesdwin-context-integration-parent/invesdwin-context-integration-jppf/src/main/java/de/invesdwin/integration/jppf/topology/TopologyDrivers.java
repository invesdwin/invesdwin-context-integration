package de.invesdwin.integration.jppf.topology;

import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;

import org.jppf.client.monitoring.topology.TopologyDriver;
import org.jppf.management.JMXDriverConnectionWrapper;
import org.jppf.management.JPPFManagementInfo;
import org.jppf.management.JPPFSystemInformation;

import de.invesdwin.context.ContextProperties;

@Immutable
public final class TopologyDrivers {

    private TopologyDrivers() {}

    public static JPPFSystemInformation extractSystemInfo(final TopologyDriver driver) {
        final JMXDriverConnectionWrapper jmx = connect(driver);
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

    public static JMXDriverConnectionWrapper connect(final TopologyDriver driver) {
        final JPPFManagementInfo managementInfo = driver.getManagementInfo();
        final String host = managementInfo.getHost();
        final int port = managementInfo.getPort();
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
