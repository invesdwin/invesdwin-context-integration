package net.sf.webdav.exceptions;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ObjectAlreadyExistsException extends WebdavException {

    private static final long serialVersionUID = 1L;

    public ObjectAlreadyExistsException() {
        super();
    }

    public ObjectAlreadyExistsException(final String message) {
        super(message);
    }

    public ObjectAlreadyExistsException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ObjectAlreadyExistsException(final Throwable cause) {
        super(cause);
    }
}
