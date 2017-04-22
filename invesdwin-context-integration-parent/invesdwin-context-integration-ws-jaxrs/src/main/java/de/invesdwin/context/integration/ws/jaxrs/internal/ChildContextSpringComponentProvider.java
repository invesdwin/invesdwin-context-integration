package de.invesdwin.context.integration.ws.jaxrs.internal;

import java.lang.annotation.Annotation;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;

import org.glassfish.hk2.api.MultiException;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.SpringComponentProvider;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.util.lang.Reflections;

@NotThreadSafe
public class ChildContextSpringComponentProvider extends SpringComponentProvider {

    @Override
    public void initialize(final ServiceLocator locator) {
        //dirty hack to get mergedContext into HK2 for jersey -.- atleast it worx
        super.initialize(new DelegateServiceLocator(locator) {
            @SuppressWarnings("unchecked")
            @Override
            public <T> T getService(final Class<T> contractOrImpl, final Annotation... qualifiers)
                    throws MultiException {
                if (contractOrImpl.equals(ServletContext.class)) {
                    return null;
                } else if (contractOrImpl.equals(ApplicationHandler.class)) {
                    final ApplicationHandler applicationHandler = (ApplicationHandler) locator.getService(
                            contractOrImpl, qualifiers);
                    Reflections.field("runtimeConfig")
                            .ofType(ResourceConfig.class)
                            .in(applicationHandler)
                            .set(new ResourceConfig());
                    applicationHandler.getConfiguration().property("contextConfig", MergedContext.getInstance());
                    return (T) applicationHandler;
                } else {
                    return super.getService(contractOrImpl, qualifiers);
                }
            }
        });
    }
}
