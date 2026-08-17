package de.invesdwin.context.integration.ws.registry.publication;

import java.net.URI;

public interface IWebServicePublication {

    boolean isValidatePort();

    String getServiceName();

    URI getUri();

    boolean isUseRegistry();

}
