package net.sf.webdav.methods;

import static net.sf.webdav.WebdavStatus.SC_CONFLICT;
import static net.sf.webdav.WebdavStatus.SC_FORBIDDEN;
import static net.sf.webdav.WebdavStatus.SC_INTERNAL_SERVER_ERROR;
import static net.sf.webdav.WebdavStatus.SC_LOCKED;
import static net.sf.webdav.WebdavStatus.SC_METHOD_NOT_ALLOWED;
import static net.sf.webdav.WebdavStatus.SC_NOT_FOUND;

import java.io.IOException;
import java.util.Hashtable;

import javax.annotation.concurrent.NotThreadSafe;

import net.sf.webdav.StoredObject;
import net.sf.webdav.WebdavStatus;
import net.sf.webdav.exceptions.AccessDeniedException;
import net.sf.webdav.exceptions.LockFailedException;
import net.sf.webdav.exceptions.ObjectAlreadyExistsException;
import net.sf.webdav.exceptions.ObjectNotFoundException;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.locking.ResourceLocks;
import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;
import net.sf.webdav.spi.IWebdavStore;
import net.sf.webdav.util.RequestUtil;

@NotThreadSafe
public class DoCopy extends AWebdavMethod {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoCopy.class);

    private final IWebdavStore store;

    private final ResourceLocks resourceLocks;

    private final DoDelete doDelete;

    private final boolean readOnly;

    public DoCopy(final IWebdavStore store, final ResourceLocks resourceLocks, final DoDelete doDelete,
            final boolean readOnly) {
        this.store = store;
        this.resourceLocks = resourceLocks;
        this.doDelete = doDelete;
        this.readOnly = readOnly;
    }

    @Override
    public void execute(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, LockFailedException {
        LOG.trace("-- " + this.getClass().getName());

        final String path = getRelativePath(req);
        if (!this.readOnly) {

            //CHECKSTYLE:OFF
            final String tempLockOwner = "doCopy" + System.currentTimeMillis() + req.toString();
            //CHECKSTYLE:ON
            if (this.resourceLocks.lock(transaction, path, tempLockOwner, false, 0, TEMP_TIMEOUT, TEMPORARY)) {
                try {
                    if (!copyResource(transaction, req, resp)) {
                        return;
                    }
                } catch (final AccessDeniedException e) {
                    resp.sendError(WebdavStatus.SC_FORBIDDEN);
                } catch (final ObjectAlreadyExistsException e) {
                    resp.sendError(WebdavStatus.SC_CONFLICT, req.getRequestURI());
                } catch (final ObjectNotFoundException e) {
                    resp.sendError(WebdavStatus.SC_NOT_FOUND, req.getRequestURI());
                } catch (final WebdavException e) {
                    resp.sendError(WebdavStatus.SC_INTERNAL_SERVER_ERROR);
                } finally {
                    this.resourceLocks.unlockTemporaryLockedObjects(transaction, path, tempLockOwner);
                }
            } else {
                resp.sendError(WebdavStatus.SC_INTERNAL_SERVER_ERROR);
            }

        } else {
            resp.sendError(WebdavStatus.SC_FORBIDDEN);
        }

    }

    /**
     * Copy a resource.
     * 
     * @param transaction
     *            indicates that the method is within the scope of a WebDAV transaction
     * @param req
     *            Servlet request
     * @param resp
     *            Servlet response
     * @return true if the copy is successful
     * @throws WebdavException
     *             if an error in the underlying store occurs
     * @throws IOException
     *             when an error occurs while sending the response
     * @throws LockFailedException
     */
    public boolean copyResource(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws WebdavException, IOException, LockFailedException {

        // Parsing destination header
        final String destinationPath = parseDestinationHeader(req, resp);

        if (destinationPath == null) {
            return false;
        }

        final String path = getRelativePath(req);

        if (path.equals(destinationPath)) {
            resp.sendError(WebdavStatus.SC_FORBIDDEN);
            return false;
        }

        Hashtable<String, WebdavStatus> errorList = new Hashtable<String, WebdavStatus>();
        final String parentDestinationPath = getParentPath(getCleanPath(destinationPath));

        if (!checkLocks(transaction, req, resp, this.resourceLocks, parentDestinationPath)) {
            errorList.put(parentDestinationPath, SC_LOCKED);
            sendReport(req, resp, errorList);
            return false; // parentDestination is locked
        }

        if (!checkLocks(transaction, req, resp, this.resourceLocks, destinationPath)) {
            errorList.put(destinationPath, SC_LOCKED);
            sendReport(req, resp, errorList);
            return false; // destination is locked
        }

        // Parsing overwrite header

        boolean overwrite = true;
        final String overwriteHeader = req.getHeader("Overwrite");

        if (overwriteHeader != null) {
            overwrite = "T".equalsIgnoreCase(overwriteHeader);
        }

        // Overwriting the destination
        //CHECKSTYLE:OFF
        final String lockOwner = "copyResource" + System.currentTimeMillis() + req.toString();
        //CHECKSTYLE:ON

        if (this.resourceLocks.lock(transaction, destinationPath, lockOwner, false, 0, TEMP_TIMEOUT, TEMPORARY)) {
            StoredObject copySo = null;
            StoredObject destinationSo = null;
            try {
                copySo = this.store.getStoredObject(transaction, path);
                // Retrieve the resources
                if (copySo == null) {
                    resp.sendError(SC_NOT_FOUND);
                    return false;
                }

                if (copySo.isNullResource()) {
                    final String methodsAllowed = ADeterminableMethod.determineMethodsAllowed(copySo);
                    resp.addHeader("Allow", methodsAllowed);
                    resp.sendError(SC_METHOD_NOT_ALLOWED);
                    return false;
                }

                errorList = new Hashtable<String, WebdavStatus>();

                destinationSo = this.store.getStoredObject(transaction, destinationPath);

                if (overwrite) {

                    // Delete destination resource, if it exists
                    if (destinationSo != null) {
                        this.doDelete.deleteResource(transaction, destinationPath, errorList, req, resp);

                    } else {
                        resp.setStatus(WebdavStatus.SC_CREATED);
                    }
                } else {

                    // If the destination exists, then it's a conflict
                    if (destinationSo != null) {
                        resp.sendError(WebdavStatus.SC_PRECONDITION_FAILED);
                        return false;
                    } else {
                        resp.setStatus(WebdavStatus.SC_CREATED);
                    }

                }
                copy(transaction, path, destinationPath, errorList, req, resp);

                if (!errorList.isEmpty()) {
                    sendReport(req, resp, errorList);
                }

            } finally {
                this.resourceLocks.unlockTemporaryLockedObjects(transaction, destinationPath, lockOwner);
            }
        } else {
            resp.sendError(SC_INTERNAL_SERVER_ERROR);
            return false;
        }
        return true;

    }

    /**
     * copies the specified resource(s) to the specified destination. preconditions must be handled by the caller.
     * Standard status codes must be handled by the caller. a multi status report in case of errors is created here.
     * 
     * @param transaction
     *            indicates that the method is within the scope of a WebDAV transaction
     * @param sourcePath
     *            path from where to read
     * @param destinationPath
     *            path where to write
     * @param req
     *            HttpServletRequest
     * @param resp
     *            HttpServletResponse
     * @throws WebdavException
     *             if an error in the underlying store occurs
     * @throws IOException
     */
    private void copy(final ITransaction transaction, final String sourcePath, final String destinationPath,
            final Hashtable<String, WebdavStatus> errorList, final IWebdavRequest req, final IWebdavResponse resp)
            throws WebdavException, IOException {

        final StoredObject sourceSo = this.store.getStoredObject(transaction, sourcePath);
        if (sourceSo.isResource()) {
            this.store.createResource(transaction, destinationPath);
            final long resourceLength = this.store.setResourceContent(transaction, destinationPath,
                    this.store.getResourceContent(transaction, sourcePath), sourceSo.getResourceLength());

            if (resourceLength != -1) {
                final StoredObject destinationSo = this.store.getStoredObject(transaction, destinationPath);
                destinationSo.setResourceLength(resourceLength);
            }

        } else {

            if (sourceSo.isFolder()) {
                copyFolder(transaction, sourcePath, destinationPath, errorList, req, resp);
            } else {
                resp.sendError(WebdavStatus.SC_NOT_FOUND);
            }
        }
    }

    /**
     * helper method of copy() recursively copies the FOLDER at source path to destination path
     * 
     * @param transaction
     *            indicates that the method is within the scope of a WebDAV transaction
     * @param sourcePath
     *            where to read
     * @param destinationPath
     *            where to write
     * @param errorList
     *            all errors that ocurred
     * @param req
     *            HttpServletRequest
     * @param resp
     *            HttpServletResponse
     * @throws WebdavException
     *             if an error in the underlying store occurs
     */
    private void copyFolder(final ITransaction transaction, final String srcPath, final String destPath,
            final Hashtable<String, WebdavStatus> errorList, final IWebdavRequest req, final IWebdavResponse resp)
            throws WebdavException {
        final String sourcePath = getCleanPath(srcPath);
        final String destinationPath = getCleanPath(destPath);

        this.store.createFolder(transaction, destinationPath);
        boolean infiniteDepth = true;
        final String depth = req.getHeader("Depth");
        if (depth != null) {
            if ("0".equals(depth)) {
                infiniteDepth = false;
            }
        }
        if (infiniteDepth) {
            String[] children = this.store.getChildrenNames(transaction, sourcePath);
            children = children == null ? new String[] {} : children;

            StoredObject childSo;
            for (int i = children.length - 1; i >= 0; i--) {
                children[i] = ensureLeadingSlash(children[i]);

                try {
                    childSo = this.store.getStoredObject(transaction, (sourcePath + children[i]));
                    if (childSo.isResource()) {
                        this.store.createResource(transaction, destinationPath + children[i]);
                        final long resourceLength = this.store.setResourceContent(transaction,
                                destinationPath + children[i],
                                this.store.getResourceContent(transaction, sourcePath + children[i]),
                                childSo.getResourceLength());

                        if (resourceLength != -1) {
                            final StoredObject destinationSo = this.store.getStoredObject(transaction,
                                    destinationPath + children[i]);
                            destinationSo.setResourceLength(resourceLength);
                        }

                    } else {
                        copyFolder(transaction, sourcePath + children[i], destinationPath + children[i], errorList, req,
                                resp);
                    }
                } catch (final AccessDeniedException e) {
                    errorList.put(destinationPath + children[i], SC_FORBIDDEN);
                } catch (final ObjectNotFoundException e) {
                    errorList.put(destinationPath + children[i], SC_NOT_FOUND);
                } catch (final ObjectAlreadyExistsException e) {
                    errorList.put(destinationPath + children[i], SC_CONFLICT);
                } catch (final WebdavException e) {
                    errorList.put(destinationPath + children[i], SC_INTERNAL_SERVER_ERROR);
                }
            }
        }
    }

    /**
     * Parses and normalizes the destination header.
     * 
     * @param req
     *            Servlet request
     * @param resp
     *            Servlet response
     * @return destinationPath
     * @throws IOException
     *             if an error occurs while sending response
     */
    private String parseDestinationHeader(final IWebdavRequest req, final IWebdavResponse resp) throws IOException {
        String destinationPath = req.getHeader("Destination");

        if (destinationPath == null) {
            resp.sendError(WebdavStatus.SC_BAD_REQUEST);
            return null;
        }

        // Remove url encoding from destination
        destinationPath = RequestUtil.urlDecode(destinationPath, "UTF8");

        final int protocolIndex = destinationPath.indexOf("://");
        if (protocolIndex >= 0) {
            // if the Destination URL contains the protocol, we can safely
            // trim everything upto the first "/" character after "://"
            final int firstSeparator = destinationPath.indexOf("/", protocolIndex + 4);
            if (firstSeparator < 0) {
                destinationPath = "/";
            } else {
                destinationPath = destinationPath.substring(firstSeparator);
            }
        } else {
            final String hostName = req.getServerName();
            if ((hostName != null) && (destinationPath.startsWith(hostName))) {
                destinationPath = destinationPath.substring(hostName.length());
            }

            final int portIndex = destinationPath.indexOf(":");
            if (portIndex >= 0) {
                destinationPath = destinationPath.substring(portIndex);
            }

            if (destinationPath.startsWith(":")) {
                final int firstSeparator = destinationPath.indexOf("/");
                if (firstSeparator < 0) {
                    destinationPath = "/";
                } else {
                    destinationPath = destinationPath.substring(firstSeparator);
                }
            }
        }

        // Normalize destination path (remove '.' and' ..')
        destinationPath = normalize(destinationPath);

        final String contextPath = req.getContextPath();
        if ((contextPath != null) && (destinationPath.startsWith(contextPath))) {
            destinationPath = destinationPath.substring(contextPath.length());
        }

        final String pathInfo = req.getPathInfo();
        if (pathInfo != null) {
            final String servletPath = req.getServicePath();
            if ((servletPath != null) && (destinationPath.startsWith(servletPath))) {
                destinationPath = destinationPath.substring(servletPath.length());
            }
        }

        return destinationPath;
    }

    /**
     * Return a context-relative path, beginning with a "/", that represents the canonical version of the specified path
     * after ".." and "." elements are resolved out. If the specified path attempts to go outside the boundaries of the
     * current context (i.e. too many ".." path elements are present), return <code>null</code> instead.
     * 
     * @param path
     *            Path to be normalized
     * @return normalized path
     */
    protected String normalize(final String path) {

        if (path == null) {
            return null;
        }

        // Create a place for the normalized path
        String normalized = path;

        if ("/.".equals(normalized)) {
            return "/";
        }

        // Normalize the slashes and add leading slash if necessary
        if (normalized.indexOf('\\') >= 0) {
            normalized = normalized.replace('\\', '/');
        }
        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }

        // Resolve occurrences of "//" in the normalized path
        while (true) {
            final int index = normalized.indexOf("//");
            if (index < 0) {
                break;
            }
            normalized = normalized.substring(0, index) + normalized.substring(index + 1);
        }

        // Resolve occurrences of "/./" in the normalized path
        while (true) {
            final int index = normalized.indexOf("/./");
            if (index < 0) {
                break;
            }
            normalized = normalized.substring(0, index) + normalized.substring(index + 2);
        }

        // Resolve occurrences of "/../" in the normalized path
        while (true) {
            final int index = normalized.indexOf("/../");
            if (index < 0) {
                break;
            }
            if (index == 0) {
                return (null); // Trying to go outside our context
            }
            final int index2 = normalized.lastIndexOf('/', index - 1);
            normalized = normalized.substring(0, index2) + normalized.substring(index + 3);
        }

        // Return the normalized path that we have completed
        return (normalized);

    }

}
