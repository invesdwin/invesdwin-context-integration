package de.invesdwin.context.integration.grid.ignite2.node;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.grid.ignite2.instance.AConfiguredIgnite2Instance;
import de.invesdwin.context.integration.grid.ignite2.instance.ConfiguredTcpDiscoveryIpFinder;

@ThreadSafe
public final class ConfiguredIgnite2Node extends AConfiguredIgnite2Instance {

    @Override
    protected IgniteConfiguration createConfiguration() {
        final IgniteConfiguration configuration = new IgniteConfiguration();

        // Use thick client mode which does not store data, but allows running compute jobs
        configuration.setClientMode(true);

        final TcpCommunicationSpi tcpCommunicationSpi = new TcpCommunicationSpi();
        // Support running behind a firewall
        tcpCommunicationSpi.setForceClientToServerConnections(true);
        // Override communication port
        tcpCommunicationSpi.setLocalPort(Ignite2NodeProperties.NODE_COMMUNICATION_PORT);
        configuration.setCommunicationSpi(tcpCommunicationSpi);

        final TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
        tcpDiscoverySpi.setLocalAddress(IntegrationProperties.HOSTNAME);
        tcpDiscoverySpi.setLocalPort(Ignite2NodeProperties.NODE_DISCOVERY_PORT);
        tcpDiscoverySpi.setIpFinder(new ConfiguredTcpDiscoveryIpFinder());
        configuration.setDiscoverySpi(tcpDiscoverySpi);

        return configuration;
    }

    @Override
    protected int getMinimumNodesCountForWarmup() {
        return 1; // Wait for at least this thick client itself to be registered
    }

    @Override
    protected int getMinimumServersCountForWarmup() {
        return 1; // Require at least one server node to be available before warmup completes
    }
}