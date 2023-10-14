package de.invesdwin.context.integration.channel.rpc.base.client;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class RemoteExecutionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RemoteExecutionException() {
        super();
    }

    public RemoteExecutionException(final String message) {
        super(message);
    }

    public RemoteExecutionException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public RemoteExecutionException(final Throwable cause) {
        super(cause);
    }

}
