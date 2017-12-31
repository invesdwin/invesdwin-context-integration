package de.invesdwin.integration.jppf.client;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.integration.ws.registry.RegistryDestinationProvider;
import de.invesdwin.integration.jppf.JPPFClientProperties;

@ThreadSafe
@Named
public class JPPFServerDestinationProvider extends RegistryDestinationProvider {

    public JPPFServerDestinationProvider() {
        setServiceName(JPPFClientProperties.SERVICE_NAME);
    }

}
