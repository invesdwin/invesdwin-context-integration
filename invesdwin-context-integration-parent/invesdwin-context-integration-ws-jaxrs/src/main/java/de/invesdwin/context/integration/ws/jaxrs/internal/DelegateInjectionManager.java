package de.invesdwin.context.integration.ws.jaxrs.internal;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class DelegateInjectionManager implements org.glassfish.jersey.internal.inject.InjectionManager {

    private final org.glassfish.jersey.internal.inject.InjectionManager delegate;

    public DelegateInjectionManager(final org.glassfish.jersey.internal.inject.InjectionManager delegate) {
        this.delegate = delegate;
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public void inject(final Object injectMe) {
        delegate.inject(injectMe);
    }

    @Override
    public void inject(final Object injectMe, final String strategy) {
        delegate.inject(injectMe, strategy);
    }

    @Override
    public void preDestroy(final Object preDestroyMe) {
        delegate.preDestroy(preDestroyMe);
    }

    @Override
    public <U> U createAndInitialize(final Class<U> createMe) {
        return delegate.createAndInitialize(createMe);
    }

    @Override
    public void completeRegistration() {
        delegate.completeRegistration();
    }

    @Override
    public void register(final org.glassfish.jersey.internal.inject.Binding binding) {
        delegate.register(binding);
    }

    @Override
    public void register(final Iterable<org.glassfish.jersey.internal.inject.Binding> descriptors) {
        delegate.register(descriptors);
    }

    @Override
    public void register(final org.glassfish.jersey.internal.inject.Binder binder) {
        delegate.register(binder);
    }

    @Override
    public void register(final Object provider) throws IllegalArgumentException {
        delegate.register(provider);
    }

    @Override
    public boolean isRegistrable(final Class<?> clazz) {
        return delegate.isRegistrable(clazz);
    }

    @Override
    public <T> List<org.glassfish.jersey.internal.inject.ServiceHolder<T>> getAllServiceHolders(
            final Class<T> contractOrImpl, final Annotation... qualifiers) {
        return delegate.getAllServiceHolders(contractOrImpl, qualifiers);
    }

    @Override
    public <T> T getInstance(final Class<T> contractOrImpl, final Annotation... qualifiers) {
        return delegate.getInstance(contractOrImpl, qualifiers);
    }

    @Override
    public <T> T getInstance(final Class<T> contractOrImpl, final String classAnalyzer) {
        return delegate.getInstance(contractOrImpl, classAnalyzer);
    }

    @Override
    public <T> T getInstance(final Class<T> contractOrImpl) {
        return delegate.getInstance(contractOrImpl);
    }

    @Override
    public <T> T getInstance(final Type contractOrImpl) {
        return delegate.getInstance(contractOrImpl);
    }

    @Override
    public Object getInstance(final org.glassfish.jersey.internal.inject.ForeignDescriptor foreignDescriptor) {
        return delegate.getInstance(foreignDescriptor);
    }

    @Override
    public org.glassfish.jersey.internal.inject.ForeignDescriptor createForeignDescriptor(
            final org.glassfish.jersey.internal.inject.Binding binding) {
        return delegate.createForeignDescriptor(binding);
    }

    @Override
    public <T> List<T> getAllInstances(final Type contractOrImpl) {
        return delegate.getAllInstances(contractOrImpl);
    }

}
