// CHECKSTYLE:OFF
// @NotThreadSafe
package de.invesdwin.context.integration.rmi.support;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import org.springframework.lang.Nullable;

/**
 * Encapsulates a remote invocation result, holding a result value or an exception. Used for HTTP-based serialization
 * invokers.
 *
 * <p>
 * This is an SPI class, typically not used directly by applications. Can be subclassed for additional invocation
 * parameters.
 *
 * <p>
 * Both {@link RemoteInvocation} and {@link RemoteInvocationResult} are designed for use with standard Java
 * serialization as well as JavaBean-style serialization.
 *
 * @author Juergen Hoeller
 * @since 1.1
 * @see RemoteInvocation
 */
public class RemoteInvocationResult implements Serializable {

    /** Use serialVersionUID from Spring 1.1 for interoperability. */
    private static final long serialVersionUID = 2138555143707773549L;

    @Nullable
    private Object value;

    @Nullable
    private Throwable exception;

    /**
     * Create a new RemoteInvocationResult for the given result value.
     *
     * @param value
     *            the result value returned by a successful invocation of the target method
     */
    public RemoteInvocationResult(@Nullable final Object value) {
        this.value = value;
    }

    /**
     * Create a new RemoteInvocationResult for the given exception.
     *
     * @param exception
     *            the exception thrown by an unsuccessful invocation of the target method
     */
    public RemoteInvocationResult(@Nullable final Throwable exception) {
        this.exception = exception;
    }

    /**
     * Create a new RemoteInvocationResult for JavaBean-style deserialization (e.g. with Jackson).
     *
     * @see #setValue
     * @see #setException
     */
    public RemoteInvocationResult() {}

    /**
     * Set the result value returned by a successful invocation of the target method, if any.
     * <p>
     * This setter is intended for JavaBean-style deserialization. Use {@link #RemoteInvocationResult(Object)}
     * otherwise.
     *
     * @see #RemoteInvocationResult()
     */
    public void setValue(@Nullable final Object value) {
        this.value = value;
    }

    /**
     * Return the result value returned by a successful invocation of the target method, if any.
     *
     * @see #hasException
     */
    @Nullable
    public Object getValue() {
        return this.value;
    }

    /**
     * Set the exception thrown by an unsuccessful invocation of the target method, if any.
     * <p>
     * This setter is intended for JavaBean-style deserialization. Use {@link #RemoteInvocationResult(Throwable)}
     * otherwise.
     *
     * @see #RemoteInvocationResult()
     */
    public void setException(@Nullable final Throwable exception) {
        this.exception = exception;
    }

    /**
     * Return the exception thrown by an unsuccessful invocation of the target method, if any.
     *
     * @see #hasException
     */
    @Nullable
    public Throwable getException() {
        return this.exception;
    }

    /**
     * Return whether this invocation result holds an exception. If this returns {@code false}, the result value applies
     * (even if it is {@code null}).
     *
     * @see #getValue
     * @see #getException
     */
    public boolean hasException() {
        return (this.exception != null);
    }

    /**
     * Return whether this invocation result holds an InvocationTargetException, thrown by an invocation of the target
     * method itself.
     *
     * @see #hasException()
     */
    public boolean hasInvocationTargetException() {
        return (this.exception instanceof InvocationTargetException);
    }

    /**
     * Recreate the invocation result, either returning the result value in case of a successful invocation of the
     * target method, or rethrowing the exception thrown by the target method.
     *
     * @return the result value, if any
     * @throws Throwable
     *             the exception, if any
     */
    @Nullable
    public Object recreate() throws Throwable {
        if (this.exception != null) {
            Throwable exToThrow = this.exception;
            if (this.exception instanceof InvocationTargetException) {
                exToThrow = ((InvocationTargetException) this.exception).getTargetException();
            }
            RemoteInvocationUtils.fillInClientStackTraceIfPossible(exToThrow);
            throw exToThrow;
        } else {
            return this.value;
        }
    }

}
