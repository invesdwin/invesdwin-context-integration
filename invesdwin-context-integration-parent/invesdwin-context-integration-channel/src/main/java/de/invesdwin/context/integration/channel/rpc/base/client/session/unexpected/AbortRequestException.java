package de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.string.description.TextDescription;

/**
 * This exception can be used to interrupt (in a lightweight way) a request to return control to its caller.
 * 
 * Though better provide an adequate reason where and why it was thrown so that debugging is a bit easier.
 * 
 * Anyhow, stacktraces can be enabled for this exception via Throwables.setDebugStackTraceEnabled(true).
 * 
 */
@NotThreadSafe
public final class AbortRequestException extends Exception {

    private static final AbortRequestException INSTANCE = new AbortRequestException("request aborted");

    private static final long serialVersionUID = 1L;

    /**
     * We always want a message here with some interesting information about the origin, since the stacktrace is
     * disabled. At least we can then search the code for the string.
     */
    private AbortRequestException(final String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        if (Throwables.isDebugStackTraceEnabled()) {
            return super.fillInStackTrace();
        } else {
            return this; // no stack trace for performance
        }
    }

    public static AbortRequestException getInstance(final String message) {
        if (Throwables.isDebugStackTraceEnabled()) {
            return new AbortRequestException(message);
        } else {
            return INSTANCE;
        }
    }

    public static AbortRequestException getInstance(final String message, final Object arg) {
        if (Throwables.isDebugStackTraceEnabled()) {
            return new AbortRequestException(TextDescription.format(message, arg));
        } else {
            return INSTANCE;
        }
    }

    public static AbortRequestException getInstance(final String message, final Object arg1, final Object arg2) {
        if (Throwables.isDebugStackTraceEnabled()) {
            return new AbortRequestException(TextDescription.format(message, arg1, arg2));
        } else {
            return INSTANCE;
        }
    }

    public static AbortRequestException getInstance(final String message, final Object... args) {
        if (Throwables.isDebugStackTraceEnabled()) {
            return new AbortRequestException(TextDescription.format(message, args));
        } else {
            return INSTANCE;
        }
    }

    public static AbortRequestException getInstance(final Throwable cause) {
        return getInstance(cause.getMessage(), cause);
    }

    public static AbortRequestException getInstance(final String message, final Throwable cause) {
        if (Throwables.isDebugStackTraceEnabled()) {
            final AbortRequestException eof = new AbortRequestException(message);
            if (cause != null) {
                eof.initCause(cause);
            }
            return eof;
        } else {
            return INSTANCE;
        }
    }

}
