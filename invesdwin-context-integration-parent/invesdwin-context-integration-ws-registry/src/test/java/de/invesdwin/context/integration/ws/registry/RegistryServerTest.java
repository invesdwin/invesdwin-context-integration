package de.invesdwin.context.integration.ws.registry;

import java.io.IOException;
import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import org.junit.Test;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.retry.RetryLaterException;
import de.invesdwin.context.integration.ws.IntegrationWsProperties;
import de.invesdwin.context.integration.ws.registry.internal.RemoteRegistryService;
import de.invesdwin.context.integration.ws.registry.internal.ServiceBindingHeartbeatChecker;
import de.invesdwin.context.integration.ws.registry.internal.persistence.ServiceBindingDao;
import de.invesdwin.context.integration.ws.registry.internal.persistence.ServiceBindingEntity;
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
    @Inject
    private ServiceBindingDao serviceBindingDao;

    static {
        registryServerUrlPreviously = IntegrationWsProperties.getRegistryServerUri();
        IntegrationWsProperties
                .setRegistryServerUri(URIs.asUri(IntegrationProperties.WEBSERVER_BIND_URI + "/spring-web"));
    }

    @Override
    public void setUpContext(final TestContext ctx) throws Exception {
        super.setUpContext(ctx);
        publicationTest.setUpContext(ctx);
        ctx.deactivateBean(RegistryServiceStub.class);
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
        final String infoUri = IntegrationWsProperties.getRegistryServerUri() + "/registry/info";
        final String info = URIs.connect(infoUri)
                .withBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD)
                .download();
        Assertions.assertThat(info).startsWith("There are 0 Services");
        publicationTest.testPublication();
    }

    @Test
    public void testServiceBindingHeartbeatChecker() throws IOException, InterruptedException {
        final long countBindingsPreviously = serviceBindingDao.count();

        final String serviceName = "testService";
        final URI accessURI = URIs.asUri("http://localhost:8080/something");
        registry.registerServiceBinding(serviceName, accessURI);
        Assertions.assertThat(serviceBindingDao.count()).isEqualTo(countBindingsPreviously + 1);

        //should be kept
        purger.purgeOldBindings();
        Assertions.assertThat(serviceBindingDao.count()).isEqualTo(countBindingsPreviously + 1);

        final String restQueryResult = URIs
                .connect(IntegrationProperties.WEBSERVER_BIND_URI
                        + "/spring-web/registry/queryServiceBindings+testService")
                .withBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD)
                .download();
        Assertions.assertThat(restQueryResult).contains(accessURI.toString());

        final ServiceBindingEntity first = serviceBindingDao.findOneFast();
        final ServiceBinding refreshed = registry.registerServiceBinding(first.getName(), first.getAccessUri());
        Assertions.assertThat(refreshed.getUpdated()).isAfter(first.getUpdated());

        final String infoUri = IntegrationProperties.WEBSERVER_BIND_URI + "/spring-web/registry/info";
        final String restInfoResult = URIs.connect(infoUri)
                .withBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD)
                .download();
        Assertions.assertThat(restInfoResult)
                .as("Unexpected: " + restInfoResult)
                .startsWith(
                        "There is 1 Service:\n1. Service [testService] has 1 ServiceBinding:\n1.1. ServiceBinding [http://localhost:8080/something] exists since [");
        Assertions.assertThat(restInfoResult).contains(FDate.valueOf(refreshed.getUpdated()).toString());

        first.setUpdated(new FDate(0));
        serviceBindingDao.save(first);
        final String restInfoResultAgain = URIs.connect(infoUri)
                .withBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD)
                .download();
        Assertions.assertThat(restInfoResultAgain)
                .as("Unexpected: " + restInfoResultAgain)
                .startsWith(
                        "There is 1 Service:\n1. Service [testService] has 1 ServiceBinding:\n1.1. ServiceBinding [http://localhost:8080/something] exists since [");
        Assertions.assertThat(restInfoResultAgain).contains("1970-01-01T00:00:00.000");

        Assertions.assertThat(URIs.connect(infoUri).isDownloadPossible()).isFalse();
        Assertions.assertThat(URIs.connect(infoUri)
                .withBasicAuth(IntegrationWsProperties.SPRING_WEB_USER, IntegrationWsProperties.SPRING_WEB_PASSWORD)
                .isDownloadPossible()).isTrue();

        //should be deleted
        purger.purgeOldBindings();
        Assertions.assertThat(serviceBindingDao.count()).isEqualTo(countBindingsPreviously);
    }

    @Test
    public void testRemoteRegistryService() throws RetryLaterException, IOException {
        final RemoteRegistryService registryService = new RemoteRegistryService();
        Assertions.checkTrue(registryService.isAvailable());
        Assertions.checkEquals(0, registryService.queryServiceBindings(null).size());
        final String serviceName = "testService";
        final URI accessUri = URIs.asUri("http://localhost:8080/something");
        Assertions.checkNotNull(registryService.registerServiceBinding(serviceName, accessUri));
        Assertions.checkEquals(1, registryService.queryServiceBindings(null).size());
        final String serviceName2 = serviceName + "2";
        Assertions.checkNotNull(registryService.registerServiceBinding(serviceName2, accessUri));
        Assertions.checkEquals(2, registryService.queryServiceBindings(null).size());
        Assertions.checkEquals(1, registryService.queryServiceBindings(serviceName).size());
        final ServiceBinding unregistered = registryService.unregisterServiceBinding(serviceName, accessUri);
        Assertions.checkNotNull(unregistered.getDeleted());
        Assertions.checkEquals(1, registryService.queryServiceBindings(null).size());
        Assertions.checkNotNull(registryService.unregisterServiceBinding(serviceName2, accessUri));
        Assertions.checkEquals(0, registryService.queryServiceBindings(null).size());
        Assertions.checkEquals(0, registryService.queryServiceBindings(serviceName).size());
        Assertions.checkEquals(0, registryService.queryServiceBindings(serviceName2).size());
    }

}
