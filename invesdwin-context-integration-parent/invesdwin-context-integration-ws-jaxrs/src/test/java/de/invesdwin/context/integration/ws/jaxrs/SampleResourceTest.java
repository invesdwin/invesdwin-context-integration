package de.invesdwin.context.integration.ws.jaxrs;

import javax.annotation.concurrent.NotThreadSafe;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.webserver.test.WebserverTest;
import de.invesdwin.util.assertions.Assertions;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;

@NotThreadSafe
@WebserverTest
public class SampleResourceTest extends ATest {

    @Test
    public void testGetIt() {
        final Client c = ClientBuilder.newClient();
        final WebTarget target = c.target(IntegrationProperties.WEBSERVER_BIND_URI + "/jersey/");
        final String responseMsg = target.path("sampleresource")
                .request()
                .accept(MediaType.TEXT_PLAIN)
                .get(String.class);
        Assertions.checkEquals("Got it!", responseMsg);
    }

    @Test
    public void testJsonJacksonGetIt() {
        final Client c = ClientBuilder.newBuilder()
                .register(JacksonObjectMapperProvider.class)
                .register(JacksonFeature.class)
                .build();
        final WebTarget target = c.target(IntegrationProperties.WEBSERVER_BIND_URI + "/jersey/");
        final SampleValueObject responseMsg = target.path("sampleresource")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get(SampleValueObject.class);
        Assertions.checkEquals("Got it!", responseMsg.getGetIt());
    }

    @Test
    public void testJsonGetIt() {
        final Client c = ClientBuilder.newBuilder().build();
        final WebTarget target = c.target(IntegrationProperties.WEBSERVER_BIND_URI + "/jersey/");
        final SampleValueObject responseMsg = target.path("sampleresource")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .get(SampleValueObject.class);
        Assertions.checkEquals("Got it!", responseMsg.getGetIt());
    }

    @Test
    public void testXmlGetIt() {
        final Client c = ClientBuilder.newBuilder().build();
        final WebTarget target = c.target(IntegrationProperties.WEBSERVER_BIND_URI + "/jersey/");
        final SampleValueObject responseMsg = target.path("sampleresource")
                .request()
                .accept(MediaType.APPLICATION_XML)
                .get(SampleValueObject.class);
        Assertions.checkEquals("Got it!", responseMsg.getGetIt());
    }
}
