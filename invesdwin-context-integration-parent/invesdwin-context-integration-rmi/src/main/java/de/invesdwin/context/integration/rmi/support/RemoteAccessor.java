// CHECKSTYLE:OFF
// @NotThreadSafe
package de.invesdwin.context.integration.rmi.support;

import org.springframework.util.Assert;

/**
 * Abstract base class for classes that access a remote service. Provides a "serviceInterface" bean property.
 *
 * <p>
 * Note that the service interface being used will show some signs of remotability, like the granularity of method calls
 * that it offers. Furthermore, it has to have serializable arguments etc.
 *
 * <p>
 * Accessors are supposed to throw Spring's generic {@link de.invesdwin.context.integration.rmi.RemoteAccessException} in case
 * of remote invocation failure, provided that the service interface does not declare {@code java.rmi.RemoteException}.
 *
 * @author Juergen Hoeller
 * @since 13.05.2003
 * @see de.invesdwin.context.integration.rmi.RemoteAccessException
 * @see java.rmi.RemoteException
 */
public abstract class RemoteAccessor extends RemotingSupport {

    private Class<?> serviceInterface;

    /**
     * Set the interface of the service to access. The interface must be suitable for the particular service and
     * remoting strategy.
     * <p>
     * Typically required to be able to create a suitable service proxy, but can also be optional if the lookup returns
     * a typed proxy.
     */
    public void setServiceInterface(final Class<?> serviceInterface) {
        Assert.notNull(serviceInterface, "'serviceInterface' must not be null");
        Assert.isTrue(serviceInterface.isInterface(), "'serviceInterface' must be an interface");
        this.serviceInterface = serviceInterface;
    }

    /**
     * Return the interface of the service to access.
     */
    public Class<?> getServiceInterface() {
        return this.serviceInterface;
    }

}
