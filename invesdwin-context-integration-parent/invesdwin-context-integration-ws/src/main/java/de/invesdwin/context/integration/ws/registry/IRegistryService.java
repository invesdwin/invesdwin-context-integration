package de.invesdwin.context.integration.ws.registry;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import javax.xml.registry.infomodel.ServiceBinding;

public interface IRegistryService {

    String ORGANIZATION = "de.invesdwin";

    ServiceBinding registerServiceInstance(String serviceName, String accessURI) throws IOException;

    void unregisterServiceInstance(String serviceName, URI accessUri) throws IOException;

    Collection<ServiceBinding> queryServiceInstances(String serviceName) throws IOException;

    Boolean isServerReady();

    boolean isServerResponding();

    void waitIfServerNotReady();

}
