package de.invesdwin.context.integration.ws.jaxrs.internal;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.glassfish.hk2.api.ActiveDescriptor;
import org.glassfish.hk2.api.Descriptor;
import org.glassfish.hk2.api.Filter;
import org.glassfish.hk2.api.Injectee;
import org.glassfish.hk2.api.MultiException;
import org.glassfish.hk2.api.ServiceHandle;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.api.ServiceLocatorState;
import org.glassfish.hk2.api.Unqualified;

@NotThreadSafe
public class DelegateServiceLocator implements ServiceLocator {

    private final ServiceLocator delegate;

    public DelegateServiceLocator(final ServiceLocator delegate) {
        this.delegate = delegate;
    }

    @Override
    public <T> T getService(final Class<T> contractOrImpl, final Annotation... qualifiers) throws MultiException {
        return delegate.getService(contractOrImpl, qualifiers);
    }

    @Override
    public <T> T getService(final Type contractOrImpl, final Annotation... qualifiers) throws MultiException {
        return delegate.getService(contractOrImpl, qualifiers);
    }

    @Override
    public <T> T getService(final Class<T> contractOrImpl, final String name, final Annotation... qualifiers)
            throws MultiException {
        return delegate.getService(contractOrImpl, name, qualifiers);
    }

    @Override
    public <T> T getService(final Type contractOrImpl, final String name, final Annotation... qualifiers)
            throws MultiException {
        return delegate.getService(contractOrImpl, name, qualifiers);
    }

    @Override
    public <T> List<T> getAllServices(final Class<T> contractOrImpl, final Annotation... qualifiers)
            throws MultiException {
        return delegate.getAllServices(contractOrImpl, qualifiers);
    }

    @Override
    public <T> List<T> getAllServices(final Type contractOrImpl, final Annotation... qualifiers) throws MultiException {
        return delegate.getAllServices(contractOrImpl, qualifiers);
    }

    @Override
    public <T> List<T> getAllServices(final Annotation qualifier, final Annotation... qualifiers)
            throws MultiException {
        return delegate.getAllServices(qualifier, qualifiers);
    }

    @Override
    public List<?> getAllServices(final Filter searchCriteria) throws MultiException {
        return delegate.getAllServiceHandles(searchCriteria);
    }

    @Override
    public <T> ServiceHandle<T> getServiceHandle(final Class<T> contractOrImpl, final Annotation... qualifiers)
            throws MultiException {
        return delegate.getServiceHandle(contractOrImpl, qualifiers);
    }

    @Override
    public <T> ServiceHandle<T> getServiceHandle(final Type contractOrImpl, final Annotation... qualifiers)
            throws MultiException {
        return delegate.getServiceHandle(contractOrImpl, qualifiers);
    }

    @Override
    public <T> ServiceHandle<T> getServiceHandle(final Class<T> contractOrImpl, final String name,
            final Annotation... qualifiers) throws MultiException {
        return delegate.getServiceHandle(contractOrImpl, qualifiers);
    }

    @Override
    public <T> ServiceHandle<T> getServiceHandle(final Type contractOrImpl, final String name,
            final Annotation... qualifiers) throws MultiException {
        return delegate.getServiceHandle(contractOrImpl, qualifiers);
    }

    @Override
    public <T> List<ServiceHandle<T>> getAllServiceHandles(final Class<T> contractOrImpl,
            final Annotation... qualifiers) throws MultiException {
        return delegate.getAllServiceHandles(contractOrImpl, qualifiers);
    }

    @Override
    public List<ServiceHandle<?>> getAllServiceHandles(final Type contractOrImpl, final Annotation... qualifiers)
            throws MultiException {
        return delegate.getAllServiceHandles(contractOrImpl, qualifiers);
    }

    @Override
    public List<ServiceHandle<?>> getAllServiceHandles(final Annotation qualifier, final Annotation... qualifiers)
            throws MultiException {
        return delegate.getAllServiceHandles(qualifier, qualifiers);
    }

    @Override
    public List<ServiceHandle<?>> getAllServiceHandles(final Filter searchCriteria) throws MultiException {
        return delegate.getAllServiceHandles(searchCriteria);
    }

    @Override
    public List<ActiveDescriptor<?>> getDescriptors(final Filter filter) {
        return delegate.getDescriptors(filter);
    }

    @Override
    public ActiveDescriptor<?> getBestDescriptor(final Filter filter) {
        return delegate.getBestDescriptor(filter);
    }

    @Override
    public ActiveDescriptor<?> reifyDescriptor(final Descriptor descriptor, final Injectee injectee)
            throws MultiException {
        return delegate.reifyDescriptor(descriptor, injectee);
    }

    @Override
    public ActiveDescriptor<?> reifyDescriptor(final Descriptor descriptor) throws MultiException {
        return delegate.reifyDescriptor(descriptor);
    }

    @Override
    public ActiveDescriptor<?> getInjecteeDescriptor(final Injectee injectee) throws MultiException {
        return delegate.getInjecteeDescriptor(injectee);
    }

    @Override
    public <T> ServiceHandle<T> getServiceHandle(final ActiveDescriptor<T> activeDescriptor, final Injectee injectee)
            throws MultiException {
        return delegate.getServiceHandle(activeDescriptor, injectee);
    }

    @Override
    public <T> ServiceHandle<T> getServiceHandle(final ActiveDescriptor<T> activeDescriptor) throws MultiException {
        return delegate.getServiceHandle(activeDescriptor);
    }

    @Override
    public <T> T getService(final ActiveDescriptor<T> activeDescriptor, final ServiceHandle<?> root)
            throws MultiException {
        return delegate.getService(activeDescriptor, root);
    }

    @Override
    public <T> T getService(final ActiveDescriptor<T> activeDescriptor, final ServiceHandle<?> root,
            final Injectee injectee) throws MultiException {
        return delegate.getService(activeDescriptor, root, injectee);
    }

    @Override
    public String getDefaultClassAnalyzerName() {
        return delegate.getDefaultClassAnalyzerName();
    }

    @Override
    public void setDefaultClassAnalyzerName(final String defaultClassAnalyzer) {
        delegate.setDefaultClassAnalyzerName(defaultClassAnalyzer);
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public long getLocatorId() {
        return delegate.getLocatorId();
    }

    @Override
    public ServiceLocator getParent() {
        return delegate.getParent();
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public ServiceLocatorState getState() {
        return delegate.getState();
    }

    @Override
    public boolean getNeutralContextClassLoader() {
        return delegate.getNeutralContextClassLoader();
    }

    @Override
    public void setNeutralContextClassLoader(final boolean neutralContextClassLoader) {
        delegate.setNeutralContextClassLoader(neutralContextClassLoader);
    }

    @Override
    public <T> T create(final Class<T> createMe) {
        return delegate.create(createMe);
    }

    @Override
    public <T> T create(final Class<T> createMe, final String strategy) {
        return delegate.create(createMe, strategy);
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
    public void postConstruct(final Object postConstructMe) {
        delegate.postConstruct(postConstructMe);
    }

    @Override
    public void postConstruct(final Object postConstructMe, final String strategy) {
        delegate.postConstruct(postConstructMe, strategy);
    }

    @Override
    public void preDestroy(final Object preDestroyMe) {
        delegate.preDestroy(preDestroyMe);
    }

    @Override
    public void preDestroy(final Object preDestroyMe, final String strategy) {
        delegate.preDestroy(preDestroyMe, strategy);
    }

    @Override
    public <U> U createAndInitialize(final Class<U> createMe) {
        return delegate.createAndInitialize(createMe);
    }

    @Override
    public <U> U createAndInitialize(final Class<U> createMe, final String strategy) {
        return delegate.createAndInitialize(createMe, strategy);
    }

    @Override
    public Unqualified getDefaultUnqualified() {
        return delegate.getDefaultUnqualified();
    }

    @Override
    public void setDefaultUnqualified(final Unqualified arg0) {
        delegate.setDefaultUnqualified(arg0);
    }

}
