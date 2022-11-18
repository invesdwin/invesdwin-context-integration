package de.invesdwin.context.integration.ws.registry.publication.internal;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.ws.IntegrationWsProperties;
import de.invesdwin.context.integration.ws.registry.IRegistryService;
import de.invesdwin.context.integration.ws.registry.publication.IWebServicePublication;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.uri.URIs;
import jakarta.inject.Inject;
import jakarta.inject.Named;

@Named
@NotThreadSafe
public class WebServicePublicationManager implements IStartupHook {

    private final Log log = new Log(this);

    @Inject
    private IRegistryService registry;
    @Inject
    private List<IWebServicePublication> publications;

    private volatile boolean started;
    private final Map<String, URI> serviceName_accessUri = new ConcurrentHashMap<String, URI>();

    public void registerPublication(final IWebServicePublication publication) throws IOException {
        publications.add(publication);
        registerServiceInstance(publication);
    }

    public void unregisterAllPublications() {
        for (final IWebServicePublication publication : publications) {
            unregisterPublication(publication);
        }
        started = false;
    }

    public void unregisterPublication(final IWebServicePublication publication) {
        final URI accessUri = serviceName_accessUri.get(publication.getServiceName());
        if (accessUri != null) {
            log.debug("Unregistering Service: %s", publication.getUri());
            try {
                registry.unregisterServiceBinding(publication.getServiceName(), accessUri);
            } catch (final Throwable t) {
                throw new RuntimeException("At: " + accessUri, t);
            }
        }
    }

    @Override
    public void startup() throws Exception {
        for (final IWebServicePublication publication : publications) {
            registerServiceInstance(publication);
        }
        started = true;
    }

    @Scheduled(fixedDelay = IntegrationWsProperties.SERVICE_BINDING_HEARTBEAT_REFRESH_INVERVAL_MILLIS)
    public void scheduledHeartbeat() throws IOException {
        if (!started) {
            return;
        }
        for (final IWebServicePublication publication : publications) {
            registerServiceInstance(publication);
        }
    }

    private void registerServiceInstance(final IWebServicePublication publication) throws IOException {
        final URI publicationUri = publication.getUri();
        if (publicationUri != null) {
            if (URIs.connect(publicationUri).isServerResponding()) {
                if (publication.isUseRegistry()) {
                    registerServiceInstanceInRegistry(publication, publicationUri);
                } else {
                    log.debug("Starting Service: %s", publicationUri);
                }
            } else {
                log.warn("Skipping Service because server not reachable: %s", publicationUri);
            }
        }
    }

    private void registerServiceInstanceInRegistry(final IWebServicePublication publication, final URI publicationUri)
            throws IOException {
        if (serviceName_accessUri.get(publication.getServiceName()) == null) {
            log.debug("Registering Service: %s", publicationUri);
        } else {
            log.debug("Updating Service registration: %s", publicationUri);
        }
        retryRegisterServiceInstance(publication);
    }

    @Retry
    private void retryRegisterServiceInstance(final IWebServicePublication publication) throws IOException {
        final URI publicationUri = publication.getUri();
        try {
            Assertions.assertThat(registry.registerServiceBinding(publication.getServiceName(), publicationUri))
                    .isNotNull();
            serviceName_accessUri.put(publication.getServiceName(), publicationUri);
        } catch (final Throwable t) {
            throw new RuntimeException("At: " + publicationUri, t);
        }
    }

}
