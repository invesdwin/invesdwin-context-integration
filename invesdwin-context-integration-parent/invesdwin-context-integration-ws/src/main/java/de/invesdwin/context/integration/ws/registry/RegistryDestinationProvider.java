package de.invesdwin.context.integration.ws.registry;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.hook.RetryHookSupport;
import de.invesdwin.context.integration.retry.task.RetryOriginator;
import de.invesdwin.context.integration.ws.IntegrationWsProperties;
import de.invesdwin.context.integration.ws.registry.internal.RemoteRegistryService;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.lang.uri.connect.IURIsConnect;
import jakarta.inject.Inject;

@NotThreadSafe
public class RegistryDestinationProvider extends RetryHookSupport implements IDestinationProvider {

    private static final Log LOG = new Log(RegistryDestinationProvider.class);

    @Inject
    private IRegistryService registry;

    private String serviceName;
    private volatile URI cachedServiceUri;
    private volatile List<URI> cachedServiceUris;
    private volatile boolean retryWhenUnavailable = false;

    public void setServiceName(final String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    public void setRetryWhenUnavailable(final boolean retryWhenNoDestinationAvailable) {
        this.retryWhenUnavailable = retryWhenNoDestinationAvailable;
    }

    public boolean isRetryWhenUnavailable() {
        return retryWhenUnavailable;
    }

    /**
     * Synchronized so that the cache can be used effectively and no two threads try to query at the same time.
     */
    @Override
    public synchronized URI getDestination() {
        if (cachedServiceUri == null) {
            try {
                cachedServiceUri = tryGetDestination();
                LOG.debug("Service [%s] will now be called by [%s].", serviceName, cachedServiceUri);
            } catch (final IOException e) {
                throw Err.process(e);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return cachedServiceUri;
    }

    /**
     * Synchronized so that the cache can be used effectively and no two threads try to query at the same time.
     */
    @Override
    public synchronized List<URI> getDestinations() {
        if (cachedServiceUris == null) {
            try {
                cachedServiceUris = tryGetDestinations();
                LOG.debug("Service [%s] will now be called by [%s].", serviceName, cachedServiceUris);
            } catch (final IOException e) {
                throw Err.process(e);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return cachedServiceUris;
    }

    @Retry
    private List<URI> tryGetDestinations() throws IOException, InterruptedException {
        NetworkUtil.waitIfInternetNotAvailable();
        final List<URI> serviceUris = new ArrayList<URI>();
        try {
            final Collection<ServiceBinding> bindings = registry.queryServiceBindings(serviceName);
            if (bindings != null) {
                for (final ServiceBinding binding : bindings) {
                    final URI accessUri = binding.getAccessUri();
                    if (checkDownloadPossible(accessUri)) {
                        serviceUris.add(accessUri);
                    }
                }
            }
        } catch (final Throwable e) {
            if (isRetryWhenUnavailable()) {
                throw new IOException("Registry server [" + IntegrationWsProperties.getRegistryServerUri()
                        + "] seems to be unreachable.", e);
            }
        }
        if (serviceUris.isEmpty() && isRetryWhenUnavailable()) {
            throw new IOException("Service [" + serviceName + "] has no reachable instance!");
        }
        return serviceUris;
    }

    @Retry
    private URI tryGetDestination() throws IOException, InterruptedException {
        NetworkUtil.waitIfInternetNotAvailable();
        URI serviceUri = null;
        try {
            final Collection<ServiceBinding> bindings = registry.queryServiceBindings(serviceName);
            if (bindings != null) {
                for (final ServiceBinding binding : bindings) {
                    final URI accessUri = binding.getAccessUri();
                    if (checkDownloadPossible(accessUri)) {
                        serviceUri = accessUri;
                        break;
                    }
                }
            }
        } catch (final Throwable e) {
            if (isRetryWhenUnavailable()) {
                throw new IOException("Registry server [" + IntegrationWsProperties.getRegistryServerUri()
                        + "] seems to be unreachable.", e);
            }
        }
        if (serviceUri == null && isRetryWhenUnavailable()) {
            throw new IOException("Service [" + serviceName + "] has no reachable instance!");
        }
        return serviceUri;
    }

    private boolean checkDownloadPossible(final URI accessUri) {
        boolean isOk = false;
        if (accessUri != null) {
            if (Strings.equalsAnyIgnoreCase(accessUri.getScheme(), "http", "https")) {
                if (RemoteRegistryService.gateway(maybeWithBasicAuth(URIs.connect(accessUri))).isDownloadPossible()) {
                    isOk = true;
                } else {
                    //check rest service aswell
                    isOk = RemoteRegistryService
                            .gateway(maybeWithBasicAuth(URIs.connect(Strings.putSuffix(accessUri.toString(), "/"))))
                            .isDownloadPossible();
                }
            } else {
                isOk = URIs.connect(accessUri).isServerResponding();
            }
        }
        if (!isOk) {
            LOG.warn(
                    "Service [%s] is unreachable via [%s]! The instance seems to not have removed itself properly on shutdown!",
                    serviceName, accessUri);
        }
        return isOk;
    }

    protected IURIsConnect maybeWithBasicAuth(final IURIsConnect connect) {
        if (String.valueOf(connect.getUri()).contains("/spring-web/")) {
            connect.putBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD);
        }
        return connect;
    }

    @Override
    public void onBeforeRetry(final RetryOriginator originator, final int retryCount, final Throwable cause) {
        //a service might have changed location, thus reset to try again with a fresh location
        reset();
    }

    @Override
    public void reset() {
        cachedServiceUri = null;
        cachedServiceUris = null;
    }

}
