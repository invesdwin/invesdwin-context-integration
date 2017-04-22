package de.invesdwin.context.integration.ws.registry;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.xml.registry.JAXRException;
import javax.xml.registry.infomodel.Organization;
import javax.xml.registry.infomodel.Service;
import javax.xml.registry.infomodel.ServiceBinding;

import org.junit.Test;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.ws.IntegrationWsProperties;
import de.invesdwin.context.integration.ws.registry.internal.ServiceBindingHeartbeatChecker;
import de.invesdwin.context.integration.ws.registry.publication.XsdWebServicePublicationTest;
import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.context.webserver.test.WebserverTest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.time.fdate.FDate;

@ThreadSafe
@WebserverTest
public class RegistryServerTest extends APersistenceTest {

    private static URI registryServerUrlPreviously;
    private final XsdWebServicePublicationTest publicationTest = new XsdWebServicePublicationTest();
    @Inject
    private IRegistryService registry;
    @Inject
    private ServiceBindingHeartbeatChecker purger;

    static {
        registryServerUrlPreviously = IntegrationWsProperties.getRegistryServerUri();
        IntegrationWsProperties.setRegistryServerUri(URIs.asUri(IntegrationProperties.WEBSERVER_BIND_URI + "/cxf"));
    }

    @Override
    public void setUpContext(final TestContext ctx) throws Exception {
        super.setUpContext(ctx);
        publicationTest.setUpContext(ctx);
        ctx.deactivate(RegistryServiceStub.class);
    }

    @Override
    public void setUp() throws Exception {
        MergedContext.autowire(publicationTest);
    }

    @Override
    public void tearDownOnce() throws Exception {
        super.tearDownOnce();
        IntegrationWsProperties.setRegistryServerUri(registryServerUrlPreviously);
    }

    @Test
    public void testStartServer() throws Exception {
        final String wsdl = URIs.connect(IntegrationWsProperties.getRegistryServerUri() + "/inquiry?wsdl").download();
        Assertions.assertThat(wsdl).contains("name=\"UDDI_Inquiry_PortType\"");
        publicationTest.testPublication();
    }

    @Test
    public void testServiceBindingHeartbeatChecker() throws JAXRException, IOException, InterruptedException {
        JaxrHelper helper = null;
        try {
            helper = new JaxrHelper();
            final int countBindingsPreviously = getAllServiceBindings(helper).size();

            final String serviceName = "testService";
            final String accessURI = "http://localhost:8080/something";
            registry.registerServiceInstance(serviceName, accessURI);
            Assertions.assertThat(getAllServiceBindings(helper).size()).isEqualTo(countBindingsPreviously + 1);

            //should be kept
            purger.purgeOldBindings();
            Assertions.assertThat(getAllServiceBindings(helper).size()).isEqualTo(countBindingsPreviously + 1);

            final String restQueryResult = URIs
                    .connect(IntegrationProperties.WEBSERVER_BIND_URI + "/spring-web/registry/query_testService")
                    .withBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD)
                    .download();
            Assertions.assertThat(restQueryResult).isEqualTo(accessURI);

            final FDate heartbeat = new FDate(0);
            helper.refreshServiceBinding(getAllServiceBindings(helper).get(0), heartbeat);
            final String infoUri = IntegrationProperties.WEBSERVER_BIND_URI + "/spring-web/registry/info";
            final String restInfoResult = URIs.connect(infoUri)
                    .withBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD)
                    .download();
            Assertions.assertThat(restInfoResult).as("Unexpected: " + restInfoResult).startsWith(
                    "There is 1 Service:\n1. Service [testService] has 1 ServiceBinding:\n1.1. ServiceBinding [http://localhost:8080/something] exists since [1970-01-01T00:00:00.000]");

            Assertions.assertThat(URIs.connect(infoUri).isDownloadPossible()).isFalse();
            Assertions.assertThat(URIs.connect(infoUri)
                    .withBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD)
                    .isDownloadPossible()).isTrue();

            //should be deleted
            purger.purgeOldBindings();
            Assertions.assertThat(getAllServiceBindings(helper).size()).isEqualTo(countBindingsPreviously);
        } finally {
            if (helper != null) {
                helper.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<ServiceBinding> getAllServiceBindings(final JaxrHelper helper) throws JAXRException {
        final Organization org = helper.getOrganization(IRegistryService.ORGANIZATION);
        if (org == null) {
            return new ArrayList<ServiceBinding>();
        }
        final Collection<Service> services = org.getServices();
        final List<ServiceBinding> allBindings = new ArrayList<ServiceBinding>();
        for (final Service s : services) {
            final Collection<ServiceBinding> bindings = s.getServiceBindings();
            allBindings.addAll(bindings);
        }
        return allBindings;
    }
}
