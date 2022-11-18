// CHECKSTYLE:OFF
// @NotThreadSafe
package de.invesdwin.context.integration.rmi;

/**
 * RemoteAccessException subclass to be thrown in case of a lookup failure, typically if the lookup happens on demand
 * for each method invocation.
 *
 * @author Juergen Hoeller
 * @since 1.1
 */
@SuppressWarnings("serial")
public class RemoteLookupFailureException extends RemoteAccessException {

    /**
     * Constructor for RemoteLookupFailureException.
     *
     * @param msg
     *            the detail message
     */
    public RemoteLookupFailureException(final String msg) {
        super(msg);
    }

    /**
     * Constructor for RemoteLookupFailureException.
     *
     * @param msg
     *            message
     * @param cause
     *            the root cause from the remoting API in use
     */
    public RemoteLookupFailureException(final String msg, final Throwable cause) {
        super(msg, cause);
    }

}
