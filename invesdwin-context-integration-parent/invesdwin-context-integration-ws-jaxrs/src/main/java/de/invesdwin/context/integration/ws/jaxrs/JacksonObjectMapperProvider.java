package de.invesdwin.context.integration.ws.jaxrs;

import javax.annotation.concurrent.Immutable;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.invesdwin.context.integration.marshaller.MarshallerJsonJackson;
import jakarta.ws.rs.ext.ContextResolver;
import jakarta.ws.rs.ext.Provider;

@Provider
@Immutable
public class JacksonObjectMapperProvider implements ContextResolver<ObjectMapper> {

    @Override
    public ObjectMapper getContext(final Class<?> type) {
        return MarshallerJsonJackson.getInstance().getJsonMapper(false);
    }
}