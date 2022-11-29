// CHECKSTYLE:OFF
// @NotThreadSafe
package org.springframework.remoting.rmi.support;

import org.aopalliance.intercept.MethodInvocation;

/**
 * Default implementation of the {@link RemoteInvocationFactory} interface. Simply creates a new standard
 * {@link RemoteInvocation} object.
 *
 * @author Juergen Hoeller
 * @since 1.1
 */
public class DefaultRemoteInvocationFactory implements RemoteInvocationFactory {

    @Override
    public RemoteInvocation createRemoteInvocation(final MethodInvocation methodInvocation) {
        return new RemoteInvocation(methodInvocation);
    }

}
