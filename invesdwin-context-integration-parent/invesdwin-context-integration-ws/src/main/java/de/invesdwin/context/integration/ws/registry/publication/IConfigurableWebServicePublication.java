package de.invesdwin.context.integration.ws.registry.publication;

public interface IConfigurableWebServicePublication extends IWebServicePublication {

    void setServiceName(String serviceName);

    void setUseRegistry(boolean useRegistry);

}
