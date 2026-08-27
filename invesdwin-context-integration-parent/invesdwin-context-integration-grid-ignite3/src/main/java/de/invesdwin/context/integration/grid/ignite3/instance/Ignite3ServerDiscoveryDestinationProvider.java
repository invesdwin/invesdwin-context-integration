package de.invesdwin.context.integration.grid.ignite3.instance;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.grid.ignite3.Ignite3ClientProperties;
import de.invesdwin.context.integration.ws.registry.RegistryDestinationProvider;
import jakarta.inject.Named;

@ThreadSafe
@Named
public class Ignite3ServerDiscoveryDestinationProvider extends RegistryDestinationProvider {

    public Ignite3ServerDiscoveryDestinationProvider() {
        setServiceName(Ignite3ClientProperties.SERVER_DISCOVERY_SERVICE_NAME);
    }

    @Override
    public boolean isRetryWhenUnavailable() {
        return false;
    }

}
