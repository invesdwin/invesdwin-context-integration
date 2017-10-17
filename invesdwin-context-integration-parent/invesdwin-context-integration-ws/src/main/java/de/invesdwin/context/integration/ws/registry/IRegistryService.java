package de.invesdwin.context.integration.ws.registry;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import de.invesdwin.context.integration.retry.RetryLaterException;

public interface IRegistryService {

    ServiceBinding registerServiceBinding(String serviceName, URI accessUri) throws IOException;

    ServiceBinding unregisterServiceBinding(String serviceName, URI accessUri) throws IOException;

    Collection<ServiceBinding> queryServiceBindings(String serviceName) throws IOException;

    boolean isAvailable() throws RetryLaterException;

}
