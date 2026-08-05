package de.invesdwin.context.integration.grid.ignite2.server;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.grid.ignite2.registry.ConfiguredTcpDiscoveryIpFinder;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.time.date.FTimeUnit;

@ThreadSafe
public final class ConfiguredIgnite2Server implements IStartupHook, IShutdownHook {

    private static final WrappedExecutorService NODE_EXECUTOR = Executors
            .newFixedThreadPool(ConfiguredIgnite2Server.class.getSimpleName(), 1);
    private static final Log LOG = new Log(ConfiguredIgnite2Server.class);

    private boolean startupInvoked = false;
    private boolean startDelayed = false;

    private volatile Ignite ignite;

    public synchronized Ignite getNode() {
        return ignite;
    }

    private synchronized void setNode(final Ignite ignite) {
        Assertions.checkNull(this.ignite, "already started");
        this.ignite = ignite;
        if (ignite != null) {
            LOG.info("%s started with name: %s", ConfiguredIgnite2Server.class.getSimpleName(), ignite.name());
        }
    }

    public synchronized void start() {
        Assertions.checkNull(ignite, "already started");

        if (!startupInvoked) {
            startDelayed = true;
            return;
        }

        NODE_EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                final IgniteConfiguration configuration = new IgniteConfiguration();

                //start a full server node which stores data and allows to run compute jobs
                configuration.setClientMode(false);

                final ClientConnectorConfiguration clientConnectorConfiguration = new ClientConnectorConfiguration();
                clientConnectorConfiguration.setHost(IntegrationProperties.HOSTNAME);
                clientConnectorConfiguration.setPort(Ignite2ServerProperties.THIN_CLIENT_PORT);
                configuration.setClientConnectorConfiguration(clientConnectorConfiguration);

                final TcpCommunicationSpi tcpCommunicationSpi = new TcpCommunicationSpi();
                //data storage requires bidirectional communication, so we do not force client to server connections
                tcpCommunicationSpi.setForceClientToServerConnections(false);
                configuration.setCommunicationSpi(tcpCommunicationSpi);

                final TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
                tcpDiscoverySpi.setLocalAddress(IntegrationProperties.HOSTNAME);
                tcpDiscoverySpi.setLocalPort(Ignite2ServerProperties.NODE_DISCOVERY_PORT);
                tcpDiscoverySpi.setIpFinder(new ConfiguredTcpDiscoveryIpFinder());
                configuration.setDiscoverySpi(tcpDiscoverySpi);

                // Ignition.start blocks until the node joins the topology
                final Ignite startedNode = Ignition.start(configuration);
                setNode(startedNode);
            }
        });

        while (ignite == null) {
            try {
                FTimeUnit.SECONDS.sleep(1);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Ignite to start", e);
            }
        }
    }

    @Override
    public synchronized void startup() throws Exception {
        startupInvoked = true;
        if (startDelayed) {
            start();
        }
    }

    public synchronized void stop() {
        if (ignite != null) {
            final String igniteName = ignite.name();

            // Cleanly shuts down this specific Ignite instance.
            // Ignite handles its own internal classloader and thread teardown cleanly.
            try {
                ignite.close();
            } catch (final Exception e) {
                LOG.error("Error shutting down Ignite node", e);
            }

            try {
                NODE_EXECUTOR.awaitPendingCountZero();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            ignite = null;
            LOG.info("%s stopped: %s", ConfiguredIgnite2Server.class.getSimpleName(), igniteName);
        }
    }

    @Override
    public void shutdown() throws Exception {
        stop();
    }
}