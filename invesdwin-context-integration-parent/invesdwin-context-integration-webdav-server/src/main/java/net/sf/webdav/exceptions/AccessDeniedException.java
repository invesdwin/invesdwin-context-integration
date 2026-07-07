package net.sf.webdav.exceptions;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class AccessDeniedException extends WebdavException {

    private static final long serialVersionUID = 1L;

    public AccessDeniedException() {
        super();
    }

    public AccessDeniedException(final String message) {
        super(message);
    }

    public AccessDeniedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public AccessDeniedException(final Throwable cause) {
        super(cause);
    }
}
