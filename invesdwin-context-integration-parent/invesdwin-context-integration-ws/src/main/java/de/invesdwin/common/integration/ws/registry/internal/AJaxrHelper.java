package de.invesdwin.common.integration.ws.registry.internal;

import java.net.PasswordAuthentication;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;
import javax.xml.registry.BulkResponse;
import javax.xml.registry.BusinessLifeCycleManager;
import javax.xml.registry.BusinessQueryManager;
import javax.xml.registry.Connection;
import javax.xml.registry.ConnectionFactory;
import javax.xml.registry.JAXRException;
import javax.xml.registry.JAXRResponse;
import javax.xml.registry.RegistryService;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.juddi.v3.client.config.UDDIClient;
import org.apache.juddi.v3.client.config.UDDIClientContainer;
import org.apache.juddi.v3.client.transport.wrapper.UDDIInquiryService;
import org.apache.juddi.v3.client.transport.wrapper.UDDIPublicationService;
import org.apache.juddi.v3.client.transport.wrapper.UDDISecurityService;
import org.apache.ws.scout.registry.ConnectionFactoryImpl;
import org.apache.ws.scout.registry.RegistryV3Impl;
import org.apache.ws.scout.transport.LocalTransport;

import de.invesdwin.common.integration.ws.IntegrationWsProperties;
import de.invesdwin.context.integration.retry.Retry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.log.error.LoggedRuntimeException;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.uri.URIs;

/**
 * This abstract class currently only exists to make the code easier to view because the single one was a bit too big.
 * 
 * This class handles the technical aspects of JAXR.
 * 
 * @author subes
 * 
 */
@NotThreadSafe
public abstract class AJaxrHelper {
    protected final Log log = new Log(this);

    protected final BusinessLifeCycleManager blm;
    protected final BusinessQueryManager bqm;
    protected final Connection conn;

    public AJaxrHelper() throws JAXRException {
        this(true);
    }

    public AJaxrHelper(final boolean waitIfServerNotReady) throws JAXRException {
        try {
            this.conn = open(waitIfServerNotReady);
            final RegistryService registryService = conn.getRegistryService();
            this.blm = registryService.getBusinessLifeCycleManager();
            this.bqm = registryService.getBusinessQueryManager();
        } catch (final JAXRException e) {
            close();
            throw e;
        }
    }

    @Retry
    public void waitIfServerNotReady() {
        final Boolean readyResponse = isServerReady();
        if (readyResponse == null) {
            throw new RetryLaterRuntimeException("Registry server is not reachable.");
        } else if (!readyResponse) {
            throw new RetryLaterRuntimeException("Registry server initializes itself currently.");
        }
    }

    public Boolean isServerReady() {
        final String readyResponse = URIs
                .connect(URIs.getBasis(IntegrationWsProperties.getRegistryServerUri()) + "/spring-web/registry/ready")
                .withBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD)
                .download();
        if (readyResponse == null) {
            return null;
        } else {
            return Boolean.parseBoolean(readyResponse);
        }
    }

    public boolean isServerResponding() {
        return isServerReady() != null;
    }

    private Connection open(final boolean waitIfServerNotReady) throws JAXRException {
        if (waitIfServerNotReady) {
            waitIfServerNotReady();
        }
        final Properties prop = new Properties();
        prop.setProperty(ConnectionFactoryImpl.QUERYMANAGER_PROPERTY, UDDIInquiryService.class.getName() + "#inquire");
        prop.setProperty(ConnectionFactoryImpl.LIFECYCLEMANAGER_PROPERTY,
                UDDIPublicationService.class.getName() + "#publish");
        prop.setProperty(ConnectionFactoryImpl.SECURITYMANAGER_PROPERTY,
                UDDISecurityService.class.getName() + "#secure");
        prop.setProperty(ConnectionFactoryImpl.TRANSPORT_CLASS_PROPERTY, LocalTransport.class.getName());
        prop.setProperty(ConnectionFactoryImpl.UDDI_VERSION_PROPERTY, RegistryV3Impl.DEFAULT_UDDI_VERSION);
        prop.setProperty(ConnectionFactoryImpl.UDDI_NAMESPACE_PROPERTY, RegistryV3Impl.DEFAULT_UDDI_NAMESPACE);
        final ConnectionFactory factory = ConnectionFactory.newInstance();
        factory.setProperties(prop);
        final Connection conn = factory.createConnection();
        conn.setCredentials(new HashSet<PasswordAuthentication>(
                Arrays.asList(new PasswordAuthentication(IntegrationWsProperties.REGISTRY_SERVER_USER,
                        IntegrationWsProperties.REGISTRY_SERVER_PASSWORD.toCharArray()))));
        synchronized (AJaxrHelper.class) {
            if (!UDDIClientContainer.contains("invesdwin")) {
                try {
                    new UDDIClient().start();
                } catch (final ConfigurationException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return conn;
    }

    public void close() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (final JAXRException e) {
            throw Err.process(e);
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> Collection<T> getResponse(final BulkResponse response) throws JAXRException {
        final Collection<Exception> exceptions = response.getExceptions();
        if (exceptions != null) {
            LoggedRuntimeException lastLoggedException = null;
            for (final Exception e : exceptions) {
                lastLoggedException = Err.process(e);
            }
            if (lastLoggedException != null) {
                throw lastLoggedException;
            }
        }
        Assertions.assertThat(response.getStatus())
                .as("Illegal JAXR response status: %s", response.getStatus())
                .isEqualTo(JAXRResponse.STATUS_SUCCESS);
        return response.getCollection();
    }

    protected <T> T getSingleResponse(final BulkResponse response) throws JAXRException {
        final Collection<T> col = getResponse(response);
        if (col.size() > 0) {
            Assertions.assertThat(col.size()).as("Only one result expected: %s", col).isEqualTo(1);
            return col.iterator().next();
        } else {
            return (T) null;
        }
    }
}
