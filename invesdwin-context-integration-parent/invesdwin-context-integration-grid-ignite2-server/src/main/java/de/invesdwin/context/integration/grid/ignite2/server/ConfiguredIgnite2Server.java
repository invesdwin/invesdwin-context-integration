package de.invesdwin.context.integration.grid.ignite2.server;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.grid.ignite2.AConfiguredIgnite2Instance;
import de.invesdwin.context.integration.grid.ignite2.registry.ConfiguredTcpDiscoveryIpFinder;

@ThreadSafe
public final class ConfiguredIgnite2Server extends AConfiguredIgnite2Instance {

    @Override
    protected IgniteConfiguration createConfiguration() {
        final IgniteConfiguration configuration = new IgniteConfiguration();

        // Start a full server node which stores data and allows running compute jobs
        configuration.setClientMode(false);

        // 1. Thin Client Port Configuration (0 = random port)
        final ClientConnectorConfiguration clientConnectorConfiguration = new ClientConnectorConfiguration();
        clientConnectorConfiguration.setHost(IntegrationProperties.HOSTNAME);
        clientConnectorConfiguration.setPort(Ignite2ServerProperties.THIN_CLIENT_PORT);
        configuration.setClientConnectorConfiguration(clientConnectorConfiguration);

        // 2. REST HTTP Port Configuration (0 = random port)
        final ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration();
        connectorConfiguration.setHost(IntegrationProperties.HOSTNAME);
        connectorConfiguration.setPort(Ignite2ServerProperties.REST_PORT);
        configuration.setConnectorConfiguration(connectorConfiguration);

        // 3. TCP Communication Port Configuration (0 = random port)
        final TcpCommunicationSpi tcpCommunicationSpi = new TcpCommunicationSpi();
        tcpCommunicationSpi.setForceClientToServerConnections(false);
        tcpCommunicationSpi.setLocalPort(Ignite2ServerProperties.SERVER_COMMUNICATION_PORT);
        configuration.setCommunicationSpi(tcpCommunicationSpi);

        // 4. TCP Discovery Port Configuration (0 = random port)
        final TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
        tcpDiscoverySpi.setLocalAddress(IntegrationProperties.HOSTNAME);
        tcpDiscoverySpi.setLocalPort(Ignite2ServerProperties.SERVER_DISCOVERY_PORT);
        tcpDiscoverySpi.setIpFinder(new ConfiguredTcpDiscoveryIpFinder());
        configuration.setDiscoverySpi(tcpDiscoverySpi);

        return configuration;
    }

    @Override
    protected int getMinimumNodesCountForWarmup() {
        return 0; // Servers don't strictly wait for clients to exist before logging warmup
    }

    @Override
    protected int getMinimumServersCountForWarmup() {
        return 1; // Wait for at least this server itself to be registered
    }
}