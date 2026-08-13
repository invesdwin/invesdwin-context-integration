package de.invesdwin.context.integration.grid.ignite2.client;

import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.apache.ignite.client.ClientAddressFinder;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.grid.ignite2.Ignite2ClientProperties;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.task.ARetryCallable;
import de.invesdwin.context.integration.retry.task.RetryOriginator;

@Immutable
public class ConfiguredClientAddressFinder implements ClientAddressFinder {

    private Ignite2ThinClientDestinationProvider destinationProvider;

    private synchronized Ignite2ThinClientDestinationProvider getDestinationProvider() {
        if (destinationProvider == null) {
            destinationProvider = MergedContext.getInstance().getBean(Ignite2ThinClientDestinationProvider.class);
        }
        return destinationProvider;
    }

    @Override
    public String[] getAddresses() {
        final ARetryCallable<String[]> retry = new ARetryCallable<String[]>(
                new RetryOriginator(ConfiguredClientAddressFinder.class, "getAddresses")) {
            @Override
            protected String[] callRetry() throws Exception {
                final Collection<URI> peers = getDestinationProvider().getDestinations();
                if ((peers == null || peers.isEmpty())) {
                    throw new RetryLaterRuntimeException(
                            "No instances of service [" + Ignite2ClientProperties.THIN_CLIENT_SERVICE_NAME + "] found");
                }
                return convertAddresses(peers);
            }
        };
        return retry.call();
    }

    private String[] convertAddresses(final Collection<URI> peers) {
        final List<String> addresses = new LinkedList<>();
        if (peers != null) {
            for (final URI peer : peers) {
                // Thin client expects standard "host:port" strings
                addresses.add(peer.getHost() + ":" + peer.getPort());
            }
        }
        return addresses.toArray(new String[0]);
    }
}