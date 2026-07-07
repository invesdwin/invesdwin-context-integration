package net.sf.webdav.exceptions;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class LockFailedException extends WebdavException {

    private static final long serialVersionUID = 1L;

    public LockFailedException() {
        super();
    }

    public LockFailedException(final String message) {
        super(message);
    }

    public LockFailedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public LockFailedException(final Throwable cause) {
        super(cause);
    }
}
