package de.invesdwin.common.integration.ws.registry.publication;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.util.lang.uri.URIs;

/**
 * Publishes a REST webservice via spring-web controller by the bean id.
 * 
 * The wsdl will get published at: http://host:port/spring-web/beanid
 * 
 * @author subes
 * 
 */
@ThreadSafe
public class RestWebServicePublication extends WebServicePublicationSupport {

    private static final String LOCATION = "/spring-web/";

    @Override
    public URI getUri() {
        return newUri(IntegrationProperties.WEBSERVER_BIND_URI, getServiceName());
    }

    public static URI newUri(final URI baseUri, final String serviceName) {
        return URIs.asUri(baseUri + LOCATION + serviceName);
    }

}
