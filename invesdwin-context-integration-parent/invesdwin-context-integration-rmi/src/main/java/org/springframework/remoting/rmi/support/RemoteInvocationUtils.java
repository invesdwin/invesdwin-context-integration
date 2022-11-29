// CHECKSTYLE:OFF
// @NotThreadSafe
package org.springframework.remoting.rmi.support;

import java.util.HashSet;
import java.util.Set;

/**
 * General utilities for handling remote invocations.
 *
 * <p>
 * Mainly intended for use within the remoting framework.
 *
 * @author Juergen Hoeller
 * @since 2.0
 */
public abstract class RemoteInvocationUtils {

    /**
     * Fill the current client-side stack trace into the given exception.
     * <p>
     * The given exception is typically thrown on the server and serialized as-is, with the client wanting it to contain
     * the client-side portion of the stack trace as well. What we can do here is to update the
     * {@code StackTraceElement} array with the current client-side stack trace, provided that we run on JDK 1.4+.
     *
     * @param ex
     *            the exception to update
     * @see Throwable#getStackTrace()
     * @see Throwable#setStackTrace(StackTraceElement[])
     */
    public static void fillInClientStackTraceIfPossible(final Throwable ex) {
        if (ex != null) {
            final StackTraceElement[] clientStack = new Throwable().getStackTrace();
            final Set<Throwable> visitedExceptions = new HashSet<>();
            Throwable exToUpdate = ex;
            while (exToUpdate != null && !visitedExceptions.contains(exToUpdate)) {
                final StackTraceElement[] serverStack = exToUpdate.getStackTrace();
                final StackTraceElement[] combinedStack = new StackTraceElement[serverStack.length
                        + clientStack.length];
                System.arraycopy(serverStack, 0, combinedStack, 0, serverStack.length);
                System.arraycopy(clientStack, 0, combinedStack, serverStack.length, clientStack.length);
                exToUpdate.setStackTrace(combinedStack);
                visitedExceptions.add(exToUpdate);
                exToUpdate = exToUpdate.getCause();
            }
        }
    }

}
