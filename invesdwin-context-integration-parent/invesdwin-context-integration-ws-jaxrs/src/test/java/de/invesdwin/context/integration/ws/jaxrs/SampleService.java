package de.invesdwin.context.integration.ws.jaxrs;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

@Immutable
@Named
public class SampleService {

    public String getPayload() {
        return "Got it!";
    }

}
