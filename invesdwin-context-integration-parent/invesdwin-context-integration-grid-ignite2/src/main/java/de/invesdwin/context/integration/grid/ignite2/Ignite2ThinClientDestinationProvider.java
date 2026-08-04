package de.invesdwin.context.integration.grid.ignite2;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.ws.registry.RegistryDestinationProvider;
import jakarta.inject.Named;

@ThreadSafe
@Named
public class Ignite2ThinClientDestinationProvider extends RegistryDestinationProvider {

    public Ignite2ThinClientDestinationProvider() {
        setServiceName(Ignite2ClientProperties.THIN_CLIENT_SERVICE_NAME);
    }

    @Override
    public boolean isRetryWhenUnavailable() {
        return false;
    }

}
