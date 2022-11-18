package de.invesdwin.context.integration.ws.jaxrs.internal;

import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.glassfish.jersey.inject.hk2.ImmediateHk2InjectionManager;
import org.glassfish.jersey.server.spi.ComponentProvider;
import org.jvnet.hk2.spring.bridge.api.SpringBridge;
import org.jvnet.hk2.spring.bridge.api.SpringIntoHK2Bridge;
import org.springframework.aop.framework.Advised;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.util.assertions.Assertions;
import jakarta.inject.Named;

@NotThreadSafe
public class MergedContextSpringComponentProvider implements ComponentProvider {

    private volatile org.glassfish.jersey.internal.inject.InjectionManager injectionManager;
    private volatile ApplicationContext ctx;

    @Override
    public void initialize(final org.glassfish.jersey.internal.inject.InjectionManager injectionManager) {
        this.injectionManager = injectionManager;

        ctx = MergedContext.getInstance();
        Assertions.checkNotNull(ctx);

        // initialize HK2 spring-bridge

        final ImmediateHk2InjectionManager hk2InjectionManager = (ImmediateHk2InjectionManager) injectionManager;
        SpringBridge.getSpringBridge().initializeSpringBridge(hk2InjectionManager.getServiceLocator());
        final SpringIntoHK2Bridge springBridge = injectionManager.getInstance(SpringIntoHK2Bridge.class);
        springBridge.bridgeSpringBeanFactory(ctx);

        injectionManager.register(
                org.glassfish.jersey.internal.inject.Bindings.injectionResolver(new AutowiredInjectResolver(ctx)));
        injectionManager.register(org.glassfish.jersey.internal.inject.Bindings.service(ctx)
                .to(ApplicationContext.class)
                .named("SpringContext"));
    }

    // detect JAX-RS classes that are also Spring @Components.
    // register these with HK2 ServiceLocator to manage their lifecycle using Spring.
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public boolean bind(final Class<?> component, final Set<Class<?>> providerContracts) {

        if (ctx == null) {
            return false;
        }

        if (AnnotationUtils.findAnnotation(component, org.springframework.stereotype.Component.class) != null
                || AnnotationUtils.findAnnotation(component, Named.class) != null) {
            final String[] beanNames = ctx.getBeanNamesForType(component);
            if (beanNames == null || beanNames.length != 1) {
                return false;
            }
            final String beanName = beanNames[0];

            final org.glassfish.jersey.internal.inject.Binding binding = org.glassfish.jersey.internal.inject.Bindings
                    .supplier(new SpringManagedBeanFactory(ctx, injectionManager, beanName))
                    .to(component)
                    .to(providerContracts);
            injectionManager.register(binding);

            return true;
        }
        return false;
    }

    @Override
    public void done() {}

    @SuppressWarnings("rawtypes")
    private static final class SpringManagedBeanFactory implements Supplier {

        private final ApplicationContext ctx;
        private final org.glassfish.jersey.internal.inject.InjectionManager injectionManager;
        private final String beanName;

        private SpringManagedBeanFactory(final ApplicationContext ctx,
                final org.glassfish.jersey.internal.inject.InjectionManager injectionManager, final String beanName) {
            this.ctx = ctx;
            this.injectionManager = injectionManager;
            this.beanName = beanName;
        }

        @Override
        public Object get() {
            final Object bean = ctx.getBean(beanName);
            if (bean instanceof Advised) {
                try {
                    // Unwrap the bean and inject the values inside of it
                    final Object localBean = ((Advised) bean).getTargetSource().getTarget();
                    injectionManager.inject(localBean);
                } catch (final Exception e) {
                    // Ignore and let the injection happen as it normally would.
                    injectionManager.inject(bean);
                }
            } else {
                injectionManager.inject(bean);
            }
            return bean;
        }
    }
}
