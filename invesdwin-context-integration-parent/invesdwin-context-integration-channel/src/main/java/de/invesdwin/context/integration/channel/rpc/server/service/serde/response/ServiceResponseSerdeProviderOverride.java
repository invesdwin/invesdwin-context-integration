package de.invesdwin.context.integration.channel.rpc.server.service.serde.response;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(METHOD)
public @interface ServiceResponseSerdeProviderOverride {

    Class<? extends IServiceResponseSerdeProvider> value();

}
