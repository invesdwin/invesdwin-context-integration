// CHECKSTYLE:OFF
// @NotThreadSafe
package org.springframework.remoting.rmi;

/**
 * RemoteAccessException subclass to be thrown when the execution of the target method failed on the server side, for
 * example when a method was not found on the target object.
 *
 * @author Juergen Hoeller
 * @since 2.5
 * @see RemoteProxyFailureException
 */
@SuppressWarnings("serial")
public class RemoteInvocationFailureException extends RemoteAccessException {

    /**
     * Constructor for RemoteInvocationFailureException.
     *
     * @param msg
     *            the detail message
     * @param cause
     *            the root cause from the remoting API in use
     */
    public RemoteInvocationFailureException(final String msg, final Throwable cause) {
        super(msg, cause);
    }

}
