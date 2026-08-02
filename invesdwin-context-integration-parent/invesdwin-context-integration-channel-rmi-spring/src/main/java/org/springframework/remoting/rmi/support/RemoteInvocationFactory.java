// CHECKSTYLE:OFF
// @NotThreadSafe
package org.springframework.remoting.rmi.support;

import org.aopalliance.intercept.MethodInvocation;

/**
 * Strategy interface for creating a {@link RemoteInvocation} from an AOP Alliance
 * {@link org.aopalliance.intercept.MethodInvocation}.
 *
 * <p>
 * Used by {@link org.springframework.remoting.rmi.rmi.RmiClientInterceptor} (for RMI invokers) and by
 * {@link org.springframework.remoting.httpinvoker.HttpInvokerClientInterceptor}.
 *
 * @author Juergen Hoeller
 * @since 1.1
 * @see DefaultRemoteInvocationFactory
 * @see org.springframework.remoting.rmi.rmi.RmiClientInterceptor#setRemoteInvocationFactory
 * @see org.springframework.remoting.httpinvoker.HttpInvokerClientInterceptor#setRemoteInvocationFactory
 */
public interface RemoteInvocationFactory {

    /**
     * Create a serializable RemoteInvocation object from the given AOP MethodInvocation.
     * <p>
     * Can be implemented to add custom context information to the remote invocation, for example user credentials.
     *
     * @param methodInvocation
     *            the original AOP MethodInvocation object
     * @return the RemoteInvocation object
     */
    RemoteInvocation createRemoteInvocation(MethodInvocation methodInvocation);

}
