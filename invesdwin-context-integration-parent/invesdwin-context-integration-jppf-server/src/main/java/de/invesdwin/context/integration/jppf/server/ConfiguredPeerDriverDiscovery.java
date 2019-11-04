package de.invesdwin.context.integration.jppf.server;

import java.net.URI;
import java.util.Collection;

import javax.annotation.concurrent.ThreadSafe;

import org.jppf.discovery.DriverConnectionInfo;
import org.jppf.discovery.PeerDriverDiscovery;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.jppf.client.ConfiguredClientDriverDiscovery;
import de.invesdwin.context.integration.jppf.client.JPPFServerDestinationProvider;
import de.invesdwin.context.integration.retry.task.ARetryingRunnable;
import de.invesdwin.context.integration.retry.task.RetryOriginator;

// http://www.jppf.org/doc/5.2/index.php?title=Custom_discovery_of_peer_drivers
@ThreadSafe
public class ConfiguredPeerDriverDiscovery extends PeerDriverDiscovery {

    public static final long REFRESH_INTERVAL_MILLIS = ConfiguredClientDriverDiscovery.REFRESH_INTERVAL_MILLIS;

    private JPPFServerDestinationProvider destinationProvider;

    private boolean shutdown;

    @Override
    public void discover() throws InterruptedException {
        MergedContext.awaitBootstrapFinished();
        final ARetryingRunnable retry = new ARetryingRunnable(
                new RetryOriginator(ConfiguredClientDriverDiscovery.class, "discover")) {
            @Override
            protected void runRetryable() throws Exception {
                while (!isShutdown()) {
                    final Collection<URI> peers = getDestinationProvider().getDestinations();
                    for (final URI peer : peers) {
                        if (!peer.equals(JPPFServerProperties.getServerBindUri())) {
                            final DriverConnectionInfo info = new DriverConnectionInfo(peer.toString(),
                                    JPPFServerProperties.PEER_SSL_ENABLED, peer.getHost(), peer.getPort());
                            newConnection(info);
                        }
                    }
                    synchronized (this) { // wait a few seconds before the next lookup
                        wait(REFRESH_INTERVAL_MILLIS);
                    }
                }
            }
        };
        retry.run();
    }

    private JPPFServerDestinationProvider getDestinationProvider() {
        if (destinationProvider == null) {
            destinationProvider = MergedContext.getInstance().getBean(JPPFServerDestinationProvider.class);
        }
        return destinationProvider;
    }

    public synchronized boolean isShutdown() {
        return shutdown;
    }

    @Override
    public synchronized void shutdown() {
        shutdown = true;
        notify(); // wake up the discover() thread
    }

}
