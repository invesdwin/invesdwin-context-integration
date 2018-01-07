package de.invesdwin.context.integration.ftp;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.integration.ws.registry.RegistryDestinationProvider;

@ThreadSafe
@Named
public class FtpServerDestinationProvider extends RegistryDestinationProvider {

    public FtpServerDestinationProvider() {
        setServiceName(FtpClientProperties.SERVICE_NAME);
    }

    @Override
    public boolean isRetryWhenUnavailable() {
        return true;
    }

}
