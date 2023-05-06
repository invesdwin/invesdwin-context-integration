package de.invesdwin.context.integration.channel.rpc.server.service.serde.response;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.reflection.Reflections;

@Immutable
public final class DefaultServiceResponseSerdeProviderLookup implements IServiceResponseSerdeProviderLookup {

    public static final DefaultServiceResponseSerdeProviderLookup INSTANCE = new DefaultServiceResponseSerdeProviderLookup();

    private DefaultServiceResponseSerdeProviderLookup() {}

    @Override
    public IServiceResponseSerdeProvider lookup(final Method method) {
        final ServiceResponseSerdeProviderOverride overrideAnnotation = Reflections.getAnnotation(method,
                ServiceResponseSerdeProviderOverride.class);
        if (overrideAnnotation != null) {
            final Class<? extends IServiceResponseSerdeProvider> overrideClass = overrideAnnotation.value();
            if (overrideClass != null) {
                final Field instanceField = Reflections.findField(overrideClass, "INSTANCE");
                try {
                    if (instanceField != null) {
                        return (IServiceResponseSerdeProvider) instanceField.get(null);
                    } else {
                        return overrideClass.getConstructor().newInstance();
                    }
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return new DefaultServiceResponseSerdeProvider(method);
    }

}
