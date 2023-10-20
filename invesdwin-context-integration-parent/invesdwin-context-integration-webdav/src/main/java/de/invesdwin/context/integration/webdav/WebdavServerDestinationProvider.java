package de.invesdwin.context.integration.webdav;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.ws.registry.RegistryDestinationProvider;
import de.invesdwin.util.lang.uri.connect.IURIsConnect;
import jakarta.inject.Named;

@ThreadSafe
@Named
public class WebdavServerDestinationProvider extends RegistryDestinationProvider {

    public WebdavServerDestinationProvider() {
        setServiceName(WebdavClientProperties.SERVICE_NAME);
        setRetryWhenUnavailable(false);
    }

    @Override
    protected IURIsConnect maybeWithBasicAuth(final IURIsConnect connect) {
        return connect.putBasicAuth(WebdavClientProperties.USERNAME, WebdavClientProperties.PASSWORD);
    }

}
