package de.invesdwin.common.integration.ws.registry.publication;

import java.net.URI;

public interface IWebServicePublication {

    String getServiceName();

    void setServiceName(String serviceName);

    URI getUri();

    void setUseRegistry(final boolean useRegistry);

    boolean isUseRegistry();

}
