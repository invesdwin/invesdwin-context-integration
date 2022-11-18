package de.invesdwin.context.integration.ws.jaxrs;

import javax.annotation.concurrent.NotThreadSafe;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@NotThreadSafe
@Path("sampleresource")
@Named
public class SampleResource {

    @Inject
    private SampleService sampleService;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getIt() {
        return sampleService.getPayload();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public SampleValueObject getItJson() {
        final SampleValueObject jvo = new SampleValueObject();
        jvo.setGetIt("Got it!");
        return jvo;
    }

    @GET
    @Produces(MediaType.APPLICATION_XML)
    public SampleValueObject getItXml() {
        final SampleValueObject jvo = new SampleValueObject();
        jvo.setGetIt("Got it!");
        return jvo;
    }
}
