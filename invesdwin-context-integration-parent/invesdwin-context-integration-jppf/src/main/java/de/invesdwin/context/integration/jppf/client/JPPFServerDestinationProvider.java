package de.invesdwin.context.integration.jppf.client;

import javax.annotation.concurrent.ThreadSafe;
import jakarta.inject.Named;

import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.ws.registry.RegistryDestinationProvider;

@ThreadSafe
@Named
public class JPPFServerDestinationProvider extends RegistryDestinationProvider {

    public JPPFServerDestinationProvider() {
        setServiceName(JPPFClientProperties.SERVICE_NAME);
    }

    @Override
    public boolean isRetryWhenUnavailable() {
        return false;
    }

}
