package de.invesdwin.context.integration.rmi.support;

import org.aopalliance.intercept.MethodInvocation;
import org.springframework.lang.Nullable;

/**
 * Abstract base class for remote service accessors that are based on serialization of {@link RemoteInvocation} objects.
 *
 * Provides a "remoteInvocationFactory" property, with a {@link DefaultRemoteInvocationFactory} as default strategy.
 *
 * @author Juergen Hoeller
 * @since 1.1
 * @see #setRemoteInvocationFactory
 * @see RemoteInvocation
 * @see RemoteInvocationFactory
 * @see DefaultRemoteInvocationFactory
 */
public abstract class RemoteInvocationBasedAccessor extends UrlBasedRemoteAccessor {

    private RemoteInvocationFactory remoteInvocationFactory = new DefaultRemoteInvocationFactory();

    /**
     * Set the RemoteInvocationFactory to use for this accessor. Default is a {@link DefaultRemoteInvocationFactory}.
     * <p>
     * A custom invocation factory can add further context information to the invocation, for example user credentials.
     */
    public void setRemoteInvocationFactory(final RemoteInvocationFactory remoteInvocationFactory) {
        this.remoteInvocationFactory = (remoteInvocationFactory != null ? remoteInvocationFactory
                : new DefaultRemoteInvocationFactory());
    }

    /**
     * Return the RemoteInvocationFactory used by this accessor.
     */
    public RemoteInvocationFactory getRemoteInvocationFactory() {
        return this.remoteInvocationFactory;
    }

    /**
     * Create a new RemoteInvocation object for the given AOP method invocation.
     * <p>
     * The default implementation delegates to the configured {@link #setRemoteInvocationFactory
     * RemoteInvocationFactory}. This can be overridden in subclasses in order to provide custom RemoteInvocation
     * subclasses, containing additional invocation parameters (e.g. user credentials).
     * <p>
     * Note that it is preferable to build a custom RemoteInvocationFactory as a reusable strategy, instead of
     * overriding this method.
     *
     * @param methodInvocation
     *            the current AOP method invocation
     * @return the RemoteInvocation object
     * @see RemoteInvocationFactory#createRemoteInvocation
     */
    protected RemoteInvocation createRemoteInvocation(final MethodInvocation methodInvocation) {
        return getRemoteInvocationFactory().createRemoteInvocation(methodInvocation);
    }

    /**
     * Recreate the invocation result contained in the given RemoteInvocationResult object.
     * <p>
     * The default implementation calls the default {@code recreate()} method. This can be overridden in subclass to
     * provide custom recreation, potentially processing the returned result object.
     *
     * @param result
     *            the RemoteInvocationResult to recreate
     * @return a return value if the invocation result is a successful return
     * @throws Throwable
     *             if the invocation result is an exception
     * @see RemoteInvocationResult#recreate()
     */
    @Nullable
    protected Object recreateRemoteInvocationResult(final RemoteInvocationResult result) throws Throwable {
        return result.recreate();
    }

}
