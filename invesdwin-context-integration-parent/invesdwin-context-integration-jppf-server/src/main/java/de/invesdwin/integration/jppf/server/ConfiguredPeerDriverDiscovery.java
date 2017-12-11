package de.invesdwin.integration.jppf.server;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import org.jppf.discovery.DriverConnectionInfo;
import org.jppf.discovery.PeerDriverDiscovery;
import org.springframework.beans.factory.annotation.Configurable;

import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.ws.registry.IRegistryService;
import de.invesdwin.context.integration.ws.registry.ServiceBinding;
import de.invesdwin.integration.jppf.ConfiguredClientDriverDiscovery;

// http://www.jppf.org/doc/5.2/index.php?title=Custom_discovery_of_peer_drivers
@ThreadSafe
@Configurable
public class ConfiguredPeerDriverDiscovery extends PeerDriverDiscovery {

    public static final String SERVICE_NAME = ConfiguredClientDriverDiscovery.SERVICE_NAME;
    public static final long REFRESH_INTERVAL_MILLIS = ConfiguredClientDriverDiscovery.REFRESH_INTERVAL_MILLIS;

    @Inject
    private IRegistryService registryService;

    private boolean shutdown;

    @Retry
    @Override
    public void discover() throws InterruptedException {
        while (!isShutdown()) {
            try {
                final Collection<ServiceBinding> peers = registryService.queryServiceBindings(SERVICE_NAME);
                for (final ServiceBinding peer : peers) {
                    final URI accessUri = peer.getAccessUri();
                    final DriverConnectionInfo info = new DriverConnectionInfo(accessUri.toString(),
                            accessUri.getHost(), accessUri.getPort());
                    newConnection(info);
                }
                synchronized (this) { // wait a few seconds before the next lookup
                    wait(REFRESH_INTERVAL_MILLIS);
                }
            } catch (final IOException e) {
                throw new RetryLaterRuntimeException(e);
            }
        }
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
