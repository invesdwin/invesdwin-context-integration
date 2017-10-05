package de.invesdwin.context.integration.ws.registry.publication;

import java.net.URI;

public interface IWebServicePublication {

    String getServiceName();

    void setServiceName(String serviceName);

    URI getUri();

    void setUseRegistry(boolean useRegistry);

    boolean isUseRegistry();

}
