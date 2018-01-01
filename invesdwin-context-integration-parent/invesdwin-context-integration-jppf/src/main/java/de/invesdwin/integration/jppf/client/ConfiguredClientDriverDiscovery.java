package de.invesdwin.integration.jppf.client;

import java.net.URI;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

import org.jppf.discovery.ClientConnectionPoolInfo;
import org.jppf.discovery.ClientDriverDiscovery;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.retry.ARetryingRunnable;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.RetryOriginator;
import de.invesdwin.integration.jppf.JPPFClientProperties;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FTimeUnit;

// http://www.jppf.org/doc/5.2/index.php?title=Custom_discovery_of_remote_drivers
@ThreadSafe
public class ConfiguredClientDriverDiscovery extends ClientDriverDiscovery {

    public static final Duration REFRESH_INTERVAL = Duration.ONE_MINUTE;
    public static final long REFRESH_INTERVAL_MILLIS = REFRESH_INTERVAL.longValue(FTimeUnit.MILLISECONDS);

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
                    if (peers == null || peers.isEmpty()) {
                        throw new RetryLaterRuntimeException(
                                "No instances of service [" + JPPFClientProperties.SERVICE_NAME + "] found");
                    }
                    for (final URI peer : peers) {
                        final ClientConnectionPoolInfo info = new ClientConnectionPoolInfo(peer.toString(),
                                JPPFClientProperties.CLIENT_SSL_ENABLED, peer.getHost(), peer.getPort());
                        newConnection(info);
                    }
                    synchronized (this) { // wait a few seconds before the next lookup
                        wait(REFRESH_INTERVAL_MILLIS);
                    }
                }
            }
        };
        retry.run();
    }

    @Override
    protected void newConnection(final ClientConnectionPoolInfo info) {
        fixSystemProperties();
        super.newConnection(info);
    }

    //TODO: remove as soon as this is fixed: http://www.jppf.org/tracker/tbg/jppf/issues/JPPF-523
    private void fixSystemProperties() {
        //CHECKSTYLE:OFF
        final Properties sysProps = System.getProperties();
        //CHECKSTYKE:ON
        final Enumeration<?> en = sysProps.propertyNames();
        while (en.hasMoreElements()) {
            final String name = (String) en.nextElement();
            final String value = sysProps.getProperty(name);
            if (value == null) {
                sysProps.setProperty(name, "");
            }
        }
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
