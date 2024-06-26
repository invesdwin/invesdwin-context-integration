package de.invesdwin.context.integration.jppf.node;

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
import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.jppf.client.ConfiguredClientDriverDiscovery;
import de.invesdwin.context.integration.jppf.client.JPPFServerDestinationProvider;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.duration.Duration;

// http://www.jppf.org/doc/4.2/index.php?title=Defining_the_node_connection_strategy
@ThreadSafe
public class ConfiguredDriverConnectionStrategy implements DriverConnectionStrategy {

    public static final Duration REFRESH_INTERVAL = ConfiguredClientDriverDiscovery.REFRESH_INTERVAL;

    private JPPFServerDestinationProvider destinationProvider;

    private FDate lastRefresh = FDates.MIN_DATE;

    private final Queue<DriverConnectionInfo> queue = new LinkedBlockingQueue<>();

    @Override
    public synchronized DriverConnectionInfo nextConnectionInfo(final DriverConnectionInfo currentInfo,
            final ConnectionContext context) {
        if ((currentInfo != null) && (context.getReason() == ConnectionReason.MANAGEMENT_REQUEST)) {
            return currentInfo;
        } else {
            if (queue.isEmpty() || new Duration(lastRefresh).isGreaterThan(REFRESH_INTERVAL)) {
                queue.clear();
                queue.addAll(discoverConnections());
                lastRefresh = new FDate();
            }
            final DriverConnectionInfo info = queue.poll();
            return info;
        }
    }

    @Retry
    private Collection<? extends DriverConnectionInfo> discoverConnections() {
        final List<DriverConnectionInfo> connections = new ArrayList<>();
        final Collection<URI> peers = getDestinationProvider().getDestinations();
        if (peers == null || peers.isEmpty()) {
            throw new RetryLaterRuntimeException(
                    "No instances of service [" + JPPFClientProperties.SERVICE_NAME + "] found");
        }
        for (final URI peer : peers) {
            final DriverConnectionInfo info = new JPPFDriverConnectionInfo(JPPFNodeProperties.PEER_SSL_ENABLED,
                    peer.getHost(), peer.getPort(), -1);
            connections.add(info);
        }
        return connections;
    }

    private JPPFServerDestinationProvider getDestinationProvider() {
        if (destinationProvider == null) {
            destinationProvider = MergedContext.getInstance().getBean(JPPFServerDestinationProvider.class);
        }
        return destinationProvider;
    }

}
