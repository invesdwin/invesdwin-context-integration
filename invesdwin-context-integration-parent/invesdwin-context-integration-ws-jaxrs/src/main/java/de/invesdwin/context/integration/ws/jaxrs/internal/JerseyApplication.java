package de.invesdwin.context.integration.ws.jaxrs.internal;

import javax.annotation.concurrent.Immutable;

import org.glassfish.jersey.server.ResourceConfig;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.ws.jaxrs.JacksonObjectMapperProvider;
import de.invesdwin.util.lang.Strings;

@Immutable
public class JerseyApplication extends ResourceConfig {

    public JerseyApplication() {
        super(JacksonObjectMapperProvider.class);
        packages(ContextProperties.getBasePackages().toArray(Strings.EMPTY_ARRAY));
    }

}
