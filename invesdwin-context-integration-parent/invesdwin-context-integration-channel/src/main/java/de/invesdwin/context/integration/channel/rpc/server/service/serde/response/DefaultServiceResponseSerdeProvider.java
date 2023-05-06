package de.invesdwin.context.integration.channel.rpc.server.service.serde.response;

import java.lang.reflect.Method;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.TypeDelegateSerde;

@Immutable
public final class DefaultServiceResponseSerdeProvider implements IServiceResponseSerdeProvider {

    private final TypeDelegateSerde<Object> serde;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DefaultServiceResponseSerdeProvider(final Method method) {
        this.serde = new TypeDelegateSerde(method.getReturnType());
    }

    @Override
    public ISerde<Object> getSerde(final Object[] requestArgs) {
        return serde;
    }

}
