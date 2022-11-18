// CHECKSTYLE:OFF
// @NotThreadSafe
package de.invesdwin.context.integration.rmi.rmi;

import java.lang.reflect.InvocationTargetException;
import java.rmi.Remote;

import de.invesdwin.context.integration.rmi.support.RemoteInvocation;
import de.invesdwin.context.integration.rmi.support.RemoteInvocationBasedExporter;

/**
 * Convenient superclass for RMI-based remote exporters. Provides a facility to automatically wrap a given plain Java
 * service object with an RmiInvocationWrapper, exposing the {@link RmiInvocationHandler} remote interface.
 *
 * <p>
 * Using the RMI invoker mechanism, RMI communication operates at the {@link RmiInvocationHandler} level, sharing a
 * common invoker stub for any number of services. Service interfaces are <i>not</i> required to extend
 * {@code java.rmi.Remote} or declare {@code java.rmi.RemoteException} on all service methods. However, in and out
 * parameters still have to be serializable.
 *
 * @author Juergen Hoeller
 * @since 1.2.5
 * @see RmiServiceExporter
 * @see JndiRmiServiceExporter
 * @deprecated as of 5.3 (phasing out serialization-based remoting)
 */
@Deprecated
public abstract class RmiBasedExporter extends RemoteInvocationBasedExporter {

    /**
     * Determine the object to export: either the service object itself or a RmiInvocationWrapper in case of a non-RMI
     * service object.
     *
     * @return the RMI object to export
     * @see #setService
     * @see #setServiceInterface
     */
    protected Remote getObjectToExport() {
        // determine remote object
        if (getService() instanceof Remote
                && (getServiceInterface() == null || Remote.class.isAssignableFrom(getServiceInterface()))) {
            // conventional RMI service
            return (Remote) getService();
        } else {
            // RMI invoker
            if (logger.isDebugEnabled()) {
                logger.debug("RMI service [" + getService() + "] is an RMI invoker");
            }
            return new RmiInvocationWrapper(getProxyForService(), this);
        }
    }

    /**
     * Redefined here to be visible to RmiInvocationWrapper. Simply delegates to the corresponding superclass method.
     */
    @Override
    protected Object invoke(final RemoteInvocation invocation, final Object targetObject)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        return super.invoke(invocation, targetObject);
    }

}
