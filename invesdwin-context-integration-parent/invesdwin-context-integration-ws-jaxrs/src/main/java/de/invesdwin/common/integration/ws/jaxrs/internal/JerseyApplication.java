package de.invesdwin.common.integration.ws.jaxrs.internal;

import javax.annotation.concurrent.Immutable;

import org.glassfish.jersey.server.ResourceConfig;

import de.invesdwin.common.integration.ws.jaxrs.JacksonObjectMapperProvider;
import de.invesdwin.context.ContextProperties;

@Immutable
public class JerseyApplication extends ResourceConfig {

    public JerseyApplication() {
        super(JacksonObjectMapperProvider.class);
        packages(ContextProperties.getBasePackages().toArray(new String[0]));
    }

}
