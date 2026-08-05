package de.invesdwin.context.integration.grid.ignite2.instance;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.grid.ignite2.Ignite2ClientProperties;
import de.invesdwin.context.integration.ws.registry.RegistryDestinationProvider;
import jakarta.inject.Named;

@ThreadSafe
@Named
public class Ignite2ServerDiscoveryDestinationProvider extends RegistryDestinationProvider {

    public Ignite2ServerDiscoveryDestinationProvider() {
        setServiceName(Ignite2ClientProperties.SERVER_DISCOVERY_SERVICE_NAME);
    }

    @Override
    public boolean isRetryWhenUnavailable() {
        return false;
    }

}
