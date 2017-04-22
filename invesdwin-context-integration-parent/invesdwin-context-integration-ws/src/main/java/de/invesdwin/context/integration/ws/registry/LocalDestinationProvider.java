package de.invesdwin.context.integration.ws.registry;

import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.ws.registry.publication.XsdWebServicePublication;

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
