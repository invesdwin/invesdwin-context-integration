package de.invesdwin.context.integration.grid.ignite3.instance;

import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.beans.init.MergedContext;

@Immutable
public class ConfiguredServerAddressFinder {

    private Ignite3ServerDiscoveryDestinationProvider destinationProvider;

    private synchronized Ignite3ServerDiscoveryDestinationProvider getDestinationProvider() {
        if (destinationProvider == null) {
            destinationProvider = MergedContext.getInstance().getBean(Ignite3ServerDiscoveryDestinationProvider.class);
        }
        return destinationProvider;
    }

    public String[] getAddresses() {
        final Collection<URI> peers = getDestinationProvider().getDestinations();
        final List<String> addresses = new LinkedList<>();

        if (peers != null) {
            for (final URI peer : peers) {
                // Ignite 3 configures network nodes via standard Host and Port strings[cite: 42]
                addresses.add(peer.getHost() + ":" + peer.getPort());
            }
        }

        return addresses.toArray(new String[0]);
    }
}