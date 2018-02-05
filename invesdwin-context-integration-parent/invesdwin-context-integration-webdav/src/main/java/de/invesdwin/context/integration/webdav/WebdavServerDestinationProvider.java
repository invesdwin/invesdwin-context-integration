package de.invesdwin.context.integration.webdav;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.integration.ws.registry.RegistryDestinationProvider;

@ThreadSafe
@Named
public class WebdavServerDestinationProvider extends RegistryDestinationProvider {

    public WebdavServerDestinationProvider() {
        setServiceName(WebdavClientProperties.SERVICE_NAME);
    }

    @Override
    public boolean isRetryWhenUnavailable() {
        return true;
    }

}
