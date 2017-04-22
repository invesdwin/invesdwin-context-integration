package de.invesdwin.common.integration.ws.registry;

import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.common.integration.ws.registry.publication.XsdWebServicePublication;
import de.invesdwin.context.integration.IntegrationProperties;

@NotThreadSafe
public class LocalDestinationProvider extends RegistryDestinationProvider {

    private volatile boolean wsdl = true;

    public void setWsdl(final boolean wsdl) {
        this.wsdl = wsdl;
    }

    public boolean getWsdl() {
        return wsdl;
    }

    @Override
    public URI getDestination() {
        if (getWsdl()) {
            return XsdWebServicePublication.newUri(IntegrationProperties.WEBSERVER_BIND_URI, getServiceName());
        } else {
            return IntegrationProperties.WEBSERVER_BIND_URI;
        }
    }

}
