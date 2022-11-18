// CHECKSTYLE:OFF
// @NotThreadSafe
package de.invesdwin.context.integration.rmi.rmi;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.util.Assert;

/**
 * {@link FactoryBean} for RMI proxies, supporting both conventional RMI services and RMI invokers. Exposes the proxied
 * service for use as a bean reference, using the specified service interface. Proxies will throw Spring's unchecked
 * RemoteAccessException on remote invocation failure instead of RMI's RemoteException.
 *
 * <p>
 * The service URL must be a valid RMI URL like "rmi://localhost:1099/myservice". RMI invokers work at the
 * RmiInvocationHandler level, using the same invoker stub for any service. Service interfaces do not have to extend
 * {@code java.rmi.Remote} or throw {@code java.rmi.RemoteException}. Of course, in and out parameters have to be
 * serializable.
 *
 * <p>
 * With conventional RMI services, this proxy factory is typically used with the RMI service interface. Alternatively,
 * this factory can also proxy a remote RMI service with a matching non-RMI business interface, i.e. an interface that
 * mirrors the RMI service methods but does not declare RemoteExceptions. In the latter case, RemoteExceptions thrown by
 * the RMI stub will automatically get converted to Spring's unchecked RemoteAccessException.
 *
 * <p>
 * The major advantage of RMI, compared to Hessian, is serialization. Effectively, any serializable Java object can be
 * transported without hassle. Hessian has its own (de-)serialization mechanisms, but is HTTP-based and thus much easier
 * to setup than RMI. Alternatively, consider Spring's HTTP invoker to combine Java serialization with HTTP-based
 * transport.
 *
 * @author Juergen Hoeller
 * @since 13.05.2003
 * @see #setServiceInterface
 * @see #setServiceUrl
 * @see RmiClientInterceptor
 * @see RmiServiceExporter
 * @see java.rmi.Remote
 * @see java.rmi.RemoteException
 * @see de.invesdwin.context.integration.rmi.RemoteAccessException
 * @see org.springframework.remoting.caucho.HessianProxyFactoryBean
 * @see org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean
 * @deprecated as of 5.3 (phasing out serialization-based remoting)
 */
@Deprecated
public class RmiProxyFactoryBean extends RmiClientInterceptor implements FactoryBean<Object>, BeanClassLoaderAware {

    private Object serviceProxy;

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        final Class<?> ifc = getServiceInterface();
        Assert.notNull(ifc, "Property 'serviceInterface' is required");
        this.serviceProxy = new ProxyFactory(ifc, this).getProxy(getBeanClassLoader());
    }

    @Override
    public Object getObject() {
        return this.serviceProxy;
    }

    @Override
    public Class<?> getObjectType() {
        return getServiceInterface();
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

}
