package de.invesdwin.context.integration.ws.registry.publication;

import java.net.URI;

public interface IWebServicePublication {

    String getServiceName();

    URI getUri();

    boolean isUseRegistry();

}
