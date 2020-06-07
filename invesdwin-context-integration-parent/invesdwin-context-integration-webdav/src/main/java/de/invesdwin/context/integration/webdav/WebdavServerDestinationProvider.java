package de.invesdwin.context.integration.webdav;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.integration.ws.registry.RegistryDestinationProvider;
import de.invesdwin.util.lang.uri.connect.IURIsConnect;

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

    @Override
    protected IURIsConnect maybeWithBasicAuth(final IURIsConnect connect) {
        return connect.withBasicAuth(WebdavClientProperties.USERNAME, WebdavClientProperties.PASSWORD);
    }

}
