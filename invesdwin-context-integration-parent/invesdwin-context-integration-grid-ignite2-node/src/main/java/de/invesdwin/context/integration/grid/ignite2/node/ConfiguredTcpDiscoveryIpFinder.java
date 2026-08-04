package de.invesdwin.context.integration.grid.ignite2.node;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;

import javax.annotation.concurrent.Immutable;

import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.grid.ignite2.Ignite2NodeDiscoveryDestinationProvider;

@Immutable
public class ConfiguredTcpDiscoveryIpFinder extends TcpDiscoveryIpFinderAdapter {

    private Ignite2NodeDiscoveryDestinationProvider destinationProvider;

    public ConfiguredTcpDiscoveryIpFinder() {
        // Set to true so nodes don't unregister themselves from this finder on shutdown
        setShared(true);
    }

    private synchronized Ignite2NodeDiscoveryDestinationProvider getDestinationProvider() {
        if (destinationProvider == null) {
            destinationProvider = MergedContext.getInstance().getBean(Ignite2NodeDiscoveryDestinationProvider.class);
        }
        return destinationProvider;
    }

    @Override
    public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        final Collection<URI> peers = getDestinationProvider().getDestinations();
        final Collection<InetSocketAddress> addresses = new LinkedList<>();

        if (peers != null) {
            for (final URI peer : peers) {
                // Ignite expects InetSocketAddresses
                addresses.add(new InetSocketAddress(peer.getHost(), peer.getPort()));
            }
        }
        return addresses;
    }

    @Override
    public void registerAddresses(final Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op: Your custom registry is the source of truth, so Ignite shouldn't write to it here.
    }

    @Override
    public void unregisterAddresses(final Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op
    }
}