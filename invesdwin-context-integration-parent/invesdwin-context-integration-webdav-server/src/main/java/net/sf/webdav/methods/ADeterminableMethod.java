package net.sf.webdav.methods;

import javax.annotation.concurrent.NotThreadSafe;

import net.sf.webdav.StoredObject;

@NotThreadSafe
public abstract class ADeterminableMethod extends AWebdavMethod {

    private static final String NULL_RESOURCE_METHODS_ALLOWED = "OPTIONS, MKCOL, PUT, PROPFIND, LOCK, UNLOCK";

    private static final String RESOURCE_METHODS_ALLOWED = "OPTIONS, GET, HEAD, POST, DELETE, TRACE"
            + ", PROPPATCH, COPY, MOVE, LOCK, UNLOCK, PROPFIND";

    private static final String FOLDER_METHOD_ALLOWED = ", PUT";

    private static final String LESS_ALLOWED_METHODS = "OPTIONS, MKCOL, PUT";

    /**
     * Determines the methods normally allowed for the resource.
     * 
     * @param so
     *            StoredObject representing the resource
     * @return all allowed methods, separated by commas
     */
    protected static String determineMethodsAllowed(final StoredObject so) {

        try {
            if (so != null) {
                if (so.isNullResource()) {

                    return NULL_RESOURCE_METHODS_ALLOWED;

                } else if (so.isFolder()) {
                    return RESOURCE_METHODS_ALLOWED + FOLDER_METHOD_ALLOWED;
                }
                // else resource
                return RESOURCE_METHODS_ALLOWED;
            }
        } catch (final Exception e) {
            // we do nothing, just return less allowed methods
        }

        return LESS_ALLOWED_METHODS;
    }
}
