package net.sf.webdav.exceptions;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class UnauthenticatedException extends WebdavException {

    private static final long serialVersionUID = 1L;

    public UnauthenticatedException() {
        super();
    }

    public UnauthenticatedException(final String message) {
        super(message);
    }

    public UnauthenticatedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public UnauthenticatedException(final Throwable cause) {
        super(cause);
    }
}
