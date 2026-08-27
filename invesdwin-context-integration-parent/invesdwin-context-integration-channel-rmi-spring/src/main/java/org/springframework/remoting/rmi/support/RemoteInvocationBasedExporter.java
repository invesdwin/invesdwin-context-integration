// CHECKSTYLE:OFF
// @NotThreadSafe
package org.springframework.remoting.rmi.support;

import java.lang.reflect.InvocationTargetException;

/**
 * Abstract base class for remote service exporters that are based on deserialization of {@link RemoteInvocation}
 * objects.
 *
 * <p>
 * Provides a "remoteInvocationExecutor" property, with a {@link DefaultRemoteInvocationExecutor} as default strategy.
 *
 * @author Juergen Hoeller
 * @since 1.1
 * @see RemoteInvocationExecutor
 * @see DefaultRemoteInvocationExecutor
 */
public abstract class RemoteInvocationBasedExporter extends RemoteExporter {

    private RemoteInvocationExecutor remoteInvocationExecutor = new DefaultRemoteInvocationExecutor();

    /**
     * Set the RemoteInvocationExecutor to use for this exporter. Default is a DefaultRemoteInvocationExecutor.
     * <p>
     * A custom invocation executor can extract further context information from the invocation, for example user
     * credentials.
     */
    public void setRemoteInvocationExecutor(final RemoteInvocationExecutor remoteInvocationExecutor) {
        this.remoteInvocationExecutor = remoteInvocationExecutor;
    }

    /**
     * Return the RemoteInvocationExecutor used by this exporter.
     */
    public RemoteInvocationExecutor getRemoteInvocationExecutor() {
        return this.remoteInvocationExecutor;
    }

    /**
     * Apply the given remote invocation to the given target object. The default implementation delegates to the
     * RemoteInvocationExecutor.
     * <p>
     * Can be overridden in subclasses for custom invocation behavior, possibly for applying additional invocation
     * parameters from a custom RemoteInvocation subclass. Note that it is preferable to use a custom
     * RemoteInvocationExecutor which is a reusable strategy.
     *
     * @param invocation
     *            the remote invocation
     * @param targetObject
     *            the target object to apply the invocation to
     * @return the invocation result
     * @throws NoSuchMethodException
     *             if the method name could not be resolved
     * @throws IllegalAccessException
     *             if the method could not be accessed
     * @throws InvocationTargetException
     *             if the method invocation resulted in an exception
     * @see RemoteInvocationExecutor#invoke
     */
    protected Object invoke(final RemoteInvocation invocation, final Object targetObject)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        if (logger.isTraceEnabled()) {
            logger.trace("Executing " + invocation);
        }
        try {
            return getRemoteInvocationExecutor().invoke(invocation, targetObject);
        } catch (final NoSuchMethodException ex) {
            if (logger.isDebugEnabled()) {
                logger.debug("Could not find target method for " + invocation, ex);
            }
            throw ex;
        } catch (final IllegalAccessException ex) {
            if (logger.isDebugEnabled()) {
                logger.debug("Could not access target method for " + invocation, ex);
            }
            throw ex;
        } catch (final InvocationTargetException ex) {
            if (logger.isDebugEnabled()) {
                logger.debug("Target method failed for " + invocation, ex.getTargetException());
            }
            throw ex;
        }
    }

    /**
     * Apply the given remote invocation to the given target object, wrapping the invocation result in a serializable
     * RemoteInvocationResult object. The default implementation creates a plain RemoteInvocationResult.
     * <p>
     * Can be overridden in subclasses for custom invocation behavior, for example to return additional context
     * information. Note that this is not covered by the RemoteInvocationExecutor strategy!
     *
     * @param invocation
     *            the remote invocation
     * @param targetObject
     *            the target object to apply the invocation to
     * @return the invocation result
     * @see #invoke
     */
    protected RemoteInvocationResult invokeAndCreateResult(final RemoteInvocation invocation,
            final Object targetObject) {
        try {
            final Object value = invoke(invocation, targetObject);
            return new RemoteInvocationResult(value);
        } catch (final Throwable ex) {
            return new RemoteInvocationResult(ex);
        }
    }

}
