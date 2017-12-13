package de.invesdwin.integration.jppf.server;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import javax.annotation.concurrent.ThreadSafe;

import org.jppf.discovery.DriverConnectionInfo;
import org.jppf.discovery.PeerDriverDiscovery;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.retry.ARetryingRunnable;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.RetryOriginator;
import de.invesdwin.context.integration.ws.registry.IRegistryService;
import de.invesdwin.context.integration.ws.registry.ServiceBinding;
import de.invesdwin.integration.jppf.ConfiguredClientDriverDiscovery;

// http://www.jppf.org/doc/5.2/index.php?title=Custom_discovery_of_peer_drivers
@ThreadSafe
public class ConfiguredPeerDriverDiscovery extends PeerDriverDiscovery {

    public static final String SERVICE_NAME = ConfiguredClientDriverDiscovery.SERVICE_NAME;
    public static final long REFRESH_INTERVAL_MILLIS = ConfiguredClientDriverDiscovery.REFRESH_INTERVAL_MILLIS;

    private IRegistryService registryService;

    private boolean shutdown;

    @Override
    public void discover() throws InterruptedException {
        MergedContext.awaitBootstrapFinished();
        final ARetryingRunnable retry = new ARetryingRunnable(
                new RetryOriginator(ConfiguredClientDriverDiscovery.class, "discover")) {
            @Override
            protected void runRetryable() throws Exception {
                while (!isShutdown()) {
                    try {
                        final Collection<ServiceBinding> peers = getRegistryService()
                                .queryServiceBindings(SERVICE_NAME);
                        for (final ServiceBinding peer : peers) {
                            final URI accessUri = peer.getAccessUri();
                            if (!accessUri.equals(JPPFServerProperties.getServerBindUri())) {
                                final DriverConnectionInfo info = new DriverConnectionInfo(accessUri.toString(),
                                        JPPFServerProperties.PEER_SSL_ENABLED, accessUri.getHost(),
                                        accessUri.getPort());
                                newConnection(info);
                            }
                        }
                        synchronized (this) { // wait a few seconds before the next lookup
                            wait(REFRESH_INTERVAL_MILLIS);
                        }
                    } catch (final IOException e) {
                        throw new RetryLaterRuntimeException(e);
                    }
                }
            }
        };
        retry.run();
    }

    private IRegistryService getRegistryService() {
        if (registryService == null) {
            registryService = MergedContext.getInstance().getBean(IRegistryService.class);
        }
        return registryService;
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
