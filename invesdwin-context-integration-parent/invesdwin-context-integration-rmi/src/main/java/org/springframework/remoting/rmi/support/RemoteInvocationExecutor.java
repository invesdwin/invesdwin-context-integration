// CHECKSTYLE:OFF
// @NotThreadSafe
package org.springframework.remoting.rmi.support;

import java.lang.reflect.InvocationTargetException;

/**
 * Strategy interface for executing a {@link RemoteInvocation} on a target object.
 *
 * <p>
 * Used by {@link org.springframework.remoting.rmi.rmi.RmiServiceExporter} (for RMI invokers) and by
 * {@link org.springframework.remoting.httpinvoker.HttpInvokerServiceExporter}.
 *
 * @author Juergen Hoeller
 * @since 1.1
 * @see DefaultRemoteInvocationFactory
 * @see org.springframework.remoting.rmi.rmi.RmiServiceExporter#setRemoteInvocationExecutor
 * @see org.springframework.remoting.httpinvoker.HttpInvokerServiceExporter#setRemoteInvocationExecutor
 */
public interface RemoteInvocationExecutor {

    /**
     * Perform this invocation on the given target object. Typically called when a RemoteInvocation is received on the
     * server.
     *
     * @param invocation
     *            the RemoteInvocation
     * @param targetObject
     *            the target object to apply the invocation to
     * @return the invocation result
     * @throws NoSuchMethodException
     *             if the method name could not be resolved
     * @throws IllegalAccessException
     *             if the method could not be accessed
     * @throws InvocationTargetException
     *             if the method invocation resulted in an exception
     * @see java.lang.reflect.Method#invoke
     */
    Object invoke(RemoteInvocation invocation, Object targetObject)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException;

}
