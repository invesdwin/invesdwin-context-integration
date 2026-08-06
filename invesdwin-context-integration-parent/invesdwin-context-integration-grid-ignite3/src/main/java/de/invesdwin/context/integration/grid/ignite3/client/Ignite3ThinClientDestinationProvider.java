package de.invesdwin.context.integration.grid.ignite3.client;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.grid.ignite3.Ignite3ClientProperties;
import de.invesdwin.context.integration.ws.registry.RegistryDestinationProvider;
import jakarta.inject.Named;

@ThreadSafe
@Named
public class Ignite3ThinClientDestinationProvider extends RegistryDestinationProvider {

    public Ignite3ThinClientDestinationProvider() {
        setServiceName(Ignite3ClientProperties.THIN_CLIENT_SERVICE_NAME);
    }

    @Override
    public boolean isRetryWhenUnavailable() {
        return false;
    }

}
