package de.invesdwin.context.integration.ws.registry.publication;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.springframework.beans.factory.BeanNameAware;

import jakarta.inject.Named;

@Immutable
@Named
public class WebServicePublicationSupport implements IWebServicePublication, BeanNameAware {

    private boolean useRegistry = true;
    private String serviceName;

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public URI getUri() {
        return null;
    }

    @Override
    public void setUseRegistry(final boolean useRegistry) {
        this.useRegistry = useRegistry;
    }

    @Override
    public boolean isUseRegistry() {
        return useRegistry;
    }

    @Override
    public void setBeanName(final String name) {
        setServiceName(name);
    }

    @Override
    public void setServiceName(final String serviceName) {
        this.serviceName = serviceName;
    }

}
