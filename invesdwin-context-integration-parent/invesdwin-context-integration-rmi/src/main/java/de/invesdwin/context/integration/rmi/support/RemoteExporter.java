// CHECKSTYLE:OFF
// @NotThreadSafe
package de.invesdwin.context.integration.rmi.support;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.framework.adapter.AdvisorAdapterRegistry;
import org.springframework.aop.framework.adapter.GlobalAdvisorAdapterRegistry;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Abstract base class for classes that export a remote service. Provides "service" and "serviceInterface" bean
 * properties.
 *
 * <p>
 * Note that the service interface being used will show some signs of remotability, like the granularity of method calls
 * that it offers. Furthermore, it has to have serializable arguments etc.
 *
 * @author Juergen Hoeller
 * @since 26.12.2003
 */
public abstract class RemoteExporter extends RemotingSupport {

    private Object service;

    private Class<?> serviceInterface;

    private Boolean registerTraceInterceptor;

    private Object[] interceptors;

    /**
     * Set the service to export. Typically populated via a bean reference.
     */
    public void setService(final Object service) {
        this.service = service;
    }

    /**
     * Return the service to export.
     */
    public Object getService() {
        return this.service;
    }

    /**
     * Set the interface of the service to export. The interface must be suitable for the particular service and
     * remoting strategy.
     */
    public void setServiceInterface(final Class<?> serviceInterface) {
        Assert.notNull(serviceInterface, "'serviceInterface' must not be null");
        Assert.isTrue(serviceInterface.isInterface(), "'serviceInterface' must be an interface");
        this.serviceInterface = serviceInterface;
    }

    /**
     * Return the interface of the service to export.
     */
    public Class<?> getServiceInterface() {
        return this.serviceInterface;
    }

    /**
     * Set whether to register a RemoteInvocationTraceInterceptor for exported services. Only applied when a subclass
     * uses {@code getProxyForService} for creating the proxy to expose.
     * <p>
     * Default is "true". RemoteInvocationTraceInterceptor's most important value is that it logs exception stacktraces
     * on the server, before propagating an exception to the client. Note that RemoteInvocationTraceInterceptor will
     * <i>not</i> be registered by default if the "interceptors" property has been specified.
     *
     * @see #setInterceptors
     * @see #getProxyForService
     * @see RemoteInvocationTraceInterceptor
     */
    public void setRegisterTraceInterceptor(final boolean registerTraceInterceptor) {
        this.registerTraceInterceptor = registerTraceInterceptor;
    }

    /**
     * Set additional interceptors (or advisors) to be applied before the remote endpoint, e.g. a
     * PerformanceMonitorInterceptor.
     * <p>
     * You may specify any AOP Alliance MethodInterceptors or other Spring AOP Advices, as well as Spring AOP Advisors.
     *
     * @see #getProxyForService
     * @see org.springframework.aop.interceptor.PerformanceMonitorInterceptor
     */
    public void setInterceptors(final Object[] interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * Check whether the service reference has been set.
     *
     * @see #setService
     */
    protected void checkService() throws IllegalArgumentException {
        Assert.notNull(getService(), "Property 'service' is required");
    }

    /**
     * Check whether a service reference has been set, and whether it matches the specified service.
     *
     * @see #setServiceInterface
     * @see #setService
     */
    protected void checkServiceInterface() throws IllegalArgumentException {
        final Class<?> serviceInterface = getServiceInterface();
        Assert.notNull(serviceInterface, "Property 'serviceInterface' is required");

        final Object service = getService();
        if (service instanceof String) {
            throw new IllegalArgumentException("Service [" + service + "] is a String "
                    + "rather than an actual service reference: Have you accidentally specified "
                    + "the service bean name as value instead of as reference?");
        }
        if (!serviceInterface.isInstance(service)) {
            throw new IllegalArgumentException(
                    "Service interface [" + serviceInterface.getName() + "] needs to be implemented by service ["
                            + service + "] of class [" + service.getClass().getName() + "]");
        }
    }

    /**
     * Get a proxy for the given service object, implementing the specified service interface.
     * <p>
     * Used to export a proxy that does not expose any internals but just a specific interface intended for remote
     * access. Furthermore, a {@link RemoteInvocationTraceInterceptor} will be registered (by default).
     *
     * @return the proxy
     * @see #setServiceInterface
     * @see #setRegisterTraceInterceptor
     * @see RemoteInvocationTraceInterceptor
     */
    protected Object getProxyForService() {
        checkService();
        checkServiceInterface();

        final ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.addInterface(getServiceInterface());

        if (this.registerTraceInterceptor != null ? this.registerTraceInterceptor : this.interceptors == null) {
            proxyFactory.addAdvice(new RemoteInvocationTraceInterceptor(getExporterName()));
        }
        if (this.interceptors != null) {
            final AdvisorAdapterRegistry adapterRegistry = GlobalAdvisorAdapterRegistry.getInstance();
            for (final Object interceptor : this.interceptors) {
                proxyFactory.addAdvisor(adapterRegistry.wrap(interceptor));
            }
        }

        proxyFactory.setTarget(getService());
        proxyFactory.setOpaque(true);

        return proxyFactory.getProxy(getBeanClassLoader());
    }

    /**
     * Return a short name for this exporter. Used for tracing of remote invocations.
     * <p>
     * Default is the unqualified class name (without package). Can be overridden in subclasses.
     *
     * @see #getProxyForService
     * @see RemoteInvocationTraceInterceptor
     * @see org.springframework.util.ClassUtils#getShortName
     */
    protected String getExporterName() {
        return ClassUtils.getShortName(getClass());
    }

}
