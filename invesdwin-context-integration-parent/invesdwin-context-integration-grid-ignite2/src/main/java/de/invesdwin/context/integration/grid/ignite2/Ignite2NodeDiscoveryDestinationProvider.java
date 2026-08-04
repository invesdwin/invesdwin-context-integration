package de.invesdwin.context.integration.grid.ignite2;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.ws.registry.RegistryDestinationProvider;
import jakarta.inject.Named;

@ThreadSafe
@Named
public class Ignite2NodeDiscoveryDestinationProvider extends RegistryDestinationProvider {

    public Ignite2NodeDiscoveryDestinationProvider() {
        setServiceName(Ignite2ClientProperties.NODE_DISCOVERY_SERVICE_NAME);
    }

    @Override
    public boolean isRetryWhenUnavailable() {
        return false;
    }

}
