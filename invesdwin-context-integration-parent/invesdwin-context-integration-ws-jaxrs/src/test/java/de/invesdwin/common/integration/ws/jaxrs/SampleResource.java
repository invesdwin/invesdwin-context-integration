package de.invesdwin.common.integration.ws.jaxrs;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

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
