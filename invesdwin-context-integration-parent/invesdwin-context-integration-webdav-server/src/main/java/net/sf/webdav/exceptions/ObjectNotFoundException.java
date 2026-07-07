package net.sf.webdav.exceptions;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ObjectNotFoundException extends WebdavException {

    private static final long serialVersionUID = 1L;

    public ObjectNotFoundException() {
        super();
    }

    public ObjectNotFoundException(final String message) {
        super(message);
    }

    public ObjectNotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ObjectNotFoundException(final Throwable cause) {
        super(cause);
    }
}
