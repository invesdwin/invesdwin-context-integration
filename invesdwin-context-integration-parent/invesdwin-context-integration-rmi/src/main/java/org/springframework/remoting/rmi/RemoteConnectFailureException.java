// CHECKSTYLE:OFF
// @NotThreadSafe
package org.springframework.remoting.rmi;

/**
 * RemoteAccessException subclass to be thrown when no connection could be established with a remote service.
 *
 * @author Juergen Hoeller
 * @since 1.1
 */
@SuppressWarnings("serial")
public class RemoteConnectFailureException extends RemoteAccessException {

    /**
     * Constructor for RemoteConnectFailureException.
     *
     * @param msg
     *            the detail message
     * @param cause
     *            the root cause from the remoting API in use
     */
    public RemoteConnectFailureException(final String msg, final Throwable cause) {
        super(msg, cause);
    }

}
