// CHECKSTYLE:OFF
// @NotThreadSafe

package de.invesdwin.context.integration.rmi.rmi;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.StubNotFoundException;
import java.rmi.UnknownHostException;

import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;

import de.invesdwin.context.integration.rmi.RemoteAccessException;
import de.invesdwin.context.integration.rmi.RemoteConnectFailureException;
import de.invesdwin.context.integration.rmi.RemoteProxyFailureException;

/**
 * Factored-out methods for performing invocations within an RMI client. Can handle both RMI and non-RMI service
 * interfaces working on an RMI stub.
 *
 * <p>
 * Note: This is an SPI class, not intended to be used by applications.
 *
 * @author Juergen Hoeller
 * @since 1.1
 * @deprecated as of 5.3 (phasing out serialization-based remoting)
 */
@Deprecated
public abstract class RmiClientInterceptorUtils {

    private static final Log logger = LogFactory.getLog(RmiClientInterceptorUtils.class);

    /**
     * Perform a raw method invocation on the given RMI stub, letting reflection exceptions through as-is.
     *
     * @param invocation
     *            the AOP MethodInvocation
     * @param stub
     *            the RMI stub
     * @return the invocation result, if any
     * @throws InvocationTargetException
     *             if thrown by reflection
     */
    @Nullable
    public static Object invokeRemoteMethod(final MethodInvocation invocation, final Object stub)
            throws InvocationTargetException {

        final Method method = invocation.getMethod();
        try {
            if (method.getDeclaringClass().isInstance(stub)) {
                // directly implemented
                return method.invoke(stub, invocation.getArguments());
            } else {
                // not directly implemented
                final Method stubMethod = stub.getClass().getMethod(method.getName(), method.getParameterTypes());
                return stubMethod.invoke(stub, invocation.getArguments());
            }
        } catch (final InvocationTargetException ex) {
            throw ex;
        } catch (final NoSuchMethodException ex) {
            throw new RemoteProxyFailureException("No matching RMI stub method found for: " + method, ex);
        } catch (final Throwable ex) {
            throw new RemoteProxyFailureException("Invocation of RMI stub method failed: " + method, ex);
        }
    }

    /**
     * Wrap the given arbitrary exception that happened during remote access in either a RemoteException or a Spring
     * RemoteAccessException (if the method signature does not support RemoteException).
     * <p>
     * Only call this for remote access exceptions, not for exceptions thrown by the target service itself!
     *
     * @param method
     *            the invoked method
     * @param ex
     *            the exception that happened, to be used as cause for the RemoteAccessException or RemoteException
     * @param message
     *            the message for the RemoteAccessException respectively RemoteException
     * @return the exception to be thrown to the caller
     */
    public static Exception convertRmiAccessException(final Method method, final Throwable ex, final String message) {
        if (logger.isDebugEnabled()) {
            logger.debug(message, ex);
        }
        if (ReflectionUtils.declaresException(method, RemoteException.class)) {
            return new RemoteException(message, ex);
        } else {
            return new RemoteAccessException(message, ex);
        }
    }

    /**
     * Convert the given RemoteException that happened during remote access to Spring's RemoteAccessException if the
     * method signature does not support RemoteException. Else, return the original RemoteException.
     *
     * @param method
     *            the invoked method
     * @param ex
     *            the RemoteException that happened
     * @param serviceName
     *            the name of the service (for debugging purposes)
     * @return the exception to be thrown to the caller
     */
    public static Exception convertRmiAccessException(final Method method, final RemoteException ex,
            final String serviceName) {
        return convertRmiAccessException(method, ex, isConnectFailure(ex), serviceName);
    }

    /**
     * Convert the given RemoteException that happened during remote access to Spring's RemoteAccessException if the
     * method signature does not support RemoteException. Else, return the original RemoteException.
     *
     * @param method
     *            the invoked method
     * @param ex
     *            the RemoteException that happened
     * @param isConnectFailure
     *            whether the given exception should be considered a connect failure
     * @param serviceName
     *            the name of the service (for debugging purposes)
     * @return the exception to be thrown to the caller
     */
    public static Exception convertRmiAccessException(final Method method, final RemoteException ex,
            final boolean isConnectFailure, final String serviceName) {

        if (logger.isDebugEnabled()) {
            logger.debug("Remote service [" + serviceName + "] threw exception", ex);
        }
        if (ReflectionUtils.declaresException(method, ex.getClass())) {
            return ex;
        } else {
            if (isConnectFailure) {
                return new RemoteConnectFailureException("Could not connect to remote service [" + serviceName + "]",
                        ex);
            } else {
                return new RemoteAccessException("Could not access remote service [" + serviceName + "]", ex);
            }
        }
    }

    /**
     * Determine whether the given RMI exception indicates a connect failure.
     * <p>
     * Treats RMI's ConnectException, ConnectIOException, UnknownHostException, NoSuchObjectException and
     * StubNotFoundException as connect failure.
     *
     * @param ex
     *            the RMI exception to check
     * @return whether the exception should be treated as connect failure
     * @see java.rmi.ConnectException
     * @see java.rmi.ConnectIOException
     * @see java.rmi.UnknownHostException
     * @see java.rmi.NoSuchObjectException
     * @see java.rmi.StubNotFoundException
     */
    public static boolean isConnectFailure(final RemoteException ex) {
        return (ex instanceof ConnectException || ex instanceof ConnectIOException || ex instanceof UnknownHostException
                || ex instanceof NoSuchObjectException || ex instanceof StubNotFoundException
                || ex.getCause() instanceof SocketException);
    }

}
