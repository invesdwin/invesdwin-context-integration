package de.invesdwin.common.integration.ws.jaxrs;

import javax.annotation.concurrent.Immutable;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.invesdwin.context.integration.Marshallers;

@Provider
@Immutable
public class JacksonObjectMapperProvider implements ContextResolver<ObjectMapper> {

    @Override
    public ObjectMapper getContext(final Class<?> type) {
        return Marshallers.getInstance().getJsonObjectMapper(false);
    }
}