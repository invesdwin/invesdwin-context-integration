package de.invesdwin.context.integration.ws.registry.internal;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.BooleanUtils;
import org.springframework.util.Base64Utils;

import com.fasterxml.jackson.core.type.TypeReference;

import de.invesdwin.context.integration.marshaller.MarshallerJsonJackson;
import de.invesdwin.context.integration.retry.RetryLaterException;
import de.invesdwin.context.integration.ws.IntegrationWsProperties;
import de.invesdwin.context.integration.ws.registry.IRegistryService;
import de.invesdwin.context.integration.ws.registry.IRestRegistryService;
import de.invesdwin.context.integration.ws.registry.ServiceBinding;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.lang.uri.connect.IURIsConnect;

@Immutable
public class RemoteRegistryService implements IRegistryService, IRestRegistryService {

    private static final TypeReference<ServiceBinding> REF_SERVICE_BINDING = new TypeReference<ServiceBinding>() {
    };
    private static final TypeReference<Collection<ServiceBinding>> REF_SERVICE_BINDING_COLLECTION = new TypeReference<Collection<ServiceBinding>>() {
    };

    @Override
    public synchronized ServiceBinding registerServiceBinding(final String serviceName, final URI accessUri)
            throws IOException {
        final String serviceNameEncoded = URIs.encode(serviceName);
        final String accessUriEncoded = Base64Utils.encodeToString(accessUri.toString().getBytes());
        final String response = connect(REGISTER_SERVICE_BINDING.replace(SERVICE_NAME_PARAM, serviceNameEncoded)
                .replace(ACCESS_URI_PARAM, accessUriEncoded)).downloadThrowing();
        final ServiceBinding result = MarshallerJsonJackson.fromJson(response, REF_SERVICE_BINDING);
        return result;
    }

    @Override
    public synchronized ServiceBinding unregisterServiceBinding(final String serviceName, final URI accessUri)
            throws IOException {
        final String serviceNameEncoded = URIs.encode(serviceName);
        final String accessUriEncoded = Base64Utils.encodeToString(accessUri.toString().getBytes());
        final String response = connect(UNREGISTER_SERVICE_BINDING.replace(SERVICE_NAME_PARAM, serviceNameEncoded)
                .replace(ACCESS_URI_PARAM, accessUriEncoded)).downloadThrowing();
        final ServiceBinding result = MarshallerJsonJackson.fromJson(response, REF_SERVICE_BINDING);
        return result;
    }

    @Override
    public Collection<ServiceBinding> queryServiceBindings(final String serviceName) throws IOException {
        final String serviceNameEncoded = URIs.encode(Strings.asStringEmptyText(serviceName));
        final IURIsConnect connect = connect(QUERY_SERVICE_BINDINGS.replace(SERVICE_NAME_PARAM, serviceNameEncoded));
        final String response = connect.downloadThrowing();
        final Collection<ServiceBinding> result = MarshallerJsonJackson.fromJson(response,
                REF_SERVICE_BINDING_COLLECTION);
        return result;
    }

    @Override
    public synchronized boolean isAvailable() throws RetryLaterException {
        final String response = connect(AVAILABLE).download();
        final Boolean result = BooleanUtils.toBooleanObject(response);
        if (result == null) {
            return false;
        } else if (!result) {
            throw new RetryLaterException("Registry Server is initializing currently");
        } else {
            return true;
        }
    }

    private IURIsConnect connect(final String request) {
        return URIs.connect(getBaseUri() + "/" + request)
                .withBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD);
    }

    private String getBaseUri() {
        return Strings.putSuffix(Strings.removeEnd(IntegrationWsProperties.getRegistryServerUri().toString(), "/"),
                "/" + REGISTRY);
    }

}
