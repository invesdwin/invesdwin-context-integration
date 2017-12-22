package de.invesdwin.integration.jppf.node;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.concurrent.ThreadSafe;

import org.jppf.node.connection.ConnectionContext;
import org.jppf.node.connection.ConnectionReason;
import org.jppf.node.connection.DriverConnectionInfo;
import org.jppf.node.connection.DriverConnectionStrategy;
import org.jppf.node.connection.JPPFDriverConnectionInfo;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.ws.registry.IRegistryService;
import de.invesdwin.context.integration.ws.registry.ServiceBinding;
import de.invesdwin.integration.jppf.client.ConfiguredClientDriverDiscovery;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;

// http://www.jppf.org/doc/4.2/index.php?title=Defining_the_node_connection_strategy
@ThreadSafe
public class ConfiguredDriverConnectionStrategy implements DriverConnectionStrategy {

    public static final String SERVICE_NAME = ConfiguredClientDriverDiscovery.SERVICE_NAME;
    public static final Duration REFRESH_INTERVAL = ConfiguredClientDriverDiscovery.REFRESH_INTERVAL;

    private IRegistryService registryService;

    private FDate lastRefresh = FDate.MIN_DATE;

    private final Queue<DriverConnectionInfo> queue = new LinkedBlockingQueue<>();

    @Override
    public synchronized DriverConnectionInfo nextConnectionInfo(final DriverConnectionInfo currentInfo,
            final ConnectionContext context) {
        // if the reconnection is requested via management, keep the current driver info
        if ((currentInfo != null) && (context.getReason() == ConnectionReason.MANAGEMENT_REQUEST)) {
            return currentInfo;
        } else {
            if (new Duration(lastRefresh).isGreaterThan(REFRESH_INTERVAL)) {
                queue.clear();
                queue.addAll(discoverConnections());
                lastRefresh = new FDate();
            }
            // extract the next info from the queue
            final DriverConnectionInfo info = queue.poll();
            // put it back at the end of the queue
            queue.offer(info);
            return info;
        }
    }

    @Retry
    private Collection<? extends DriverConnectionInfo> discoverConnections() {
        try {
            final List<DriverConnectionInfo> connections = new ArrayList<>();
            final Collection<ServiceBinding> peers = getRegistryService().queryServiceBindings(SERVICE_NAME);
            if (peers == null || peers.isEmpty()) {
                throw new RetryLaterRuntimeException("No instances of service [" + SERVICE_NAME + "] found");
            }
            for (final ServiceBinding peer : peers) {
                final URI accessUri = peer.getAccessUri();
                final DriverConnectionInfo info = new JPPFDriverConnectionInfo(JPPFNodeProperties.PEER_SSL_ENABLED,
                        accessUri.getHost(), accessUri.getPort(), -1);
                connections.add(info);
            }
            return connections;
        } catch (final IOException e) {
            throw new RetryLaterRuntimeException(e);
        }
    }

    private IRegistryService getRegistryService() {
        if (registryService == null) {
            registryService = MergedContext.getInstance().getBean(IRegistryService.class);
        }
        return registryService;
    }

}
