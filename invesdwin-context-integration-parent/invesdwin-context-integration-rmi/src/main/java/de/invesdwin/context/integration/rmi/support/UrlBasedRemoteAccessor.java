package de.invesdwin.context.integration.rmi.support;

import org.springframework.beans.factory.InitializingBean;

/**
 * Abstract base class for classes that access remote services via URLs. Provides a "serviceUrl" bean property, which is
 * considered as required.
 *
 * @author Juergen Hoeller
 * @since 15.12.2003
 */
public abstract class UrlBasedRemoteAccessor extends RemoteAccessor implements InitializingBean {

    private String serviceUrl;

    /**
     * Set the URL of this remote accessor's target service. The URL must be compatible with the rules of the particular
     * remoting provider.
     */
    public void setServiceUrl(final String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    /**
     * Return the URL of this remote accessor's target service.
     */
    public String getServiceUrl() {
        return this.serviceUrl;
    }

    @Override
    public void afterPropertiesSet() {
        if (getServiceUrl() == null) {
            throw new IllegalArgumentException("Property 'serviceUrl' is required");
        }
    }

}
