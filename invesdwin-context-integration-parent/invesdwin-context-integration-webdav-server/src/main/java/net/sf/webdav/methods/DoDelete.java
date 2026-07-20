package net.sf.webdav.methods;

import static net.sf.webdav.WebdavStatus.SC_FORBIDDEN;
import static net.sf.webdav.WebdavStatus.SC_INTERNAL_SERVER_ERROR;
import static net.sf.webdav.WebdavStatus.SC_LOCKED;
import static net.sf.webdav.WebdavStatus.SC_NOT_FOUND;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

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

@NotThreadSafe
public class DoDelete extends AWebdavMethod {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoDelete.class);

    private final IWebdavStore store;

    private final ResourceLocks resourceLocks;

    private final boolean readOnly;

    public DoDelete(final IWebdavStore store, final ResourceLocks resourceLocks, final boolean readOnly) {
        this.store = store;
        this.resourceLocks = resourceLocks;
        this.readOnly = readOnly;
    }

    @Override
    public void execute(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, LockFailedException {
        LOG.trace("-- " + this.getClass().getName());

        if (!this.readOnly) {
            final String path = getRelativePath(req);
            final String parentPath = getParentPath(getCleanPath(path));

            //CHECKSTYLE:OFF
            final Map<String, WebdavStatus> errorList = new LinkedHashMap<String, WebdavStatus>();
            //CHECKSTYLE:ON

            if (!checkLocks(transaction, req, resp, this.resourceLocks, parentPath)) {
                errorList.put(parentPath, SC_LOCKED);
                sendReport(req, resp, errorList);
                return; // parent is locked
            }

            if (!checkLocks(transaction, req, resp, this.resourceLocks, path)) {
                errorList.put(path, SC_LOCKED);
                sendReport(req, resp, errorList);
                return; // resource is locked
            }

            //CHECKSTYLE:OFF
            final String tempLockOwner = "doDelete" + System.currentTimeMillis() + req.toString();
            //CHECKSTYLE:ON
            if (this.resourceLocks.lock(transaction, path, tempLockOwner, false, 0, TEMP_TIMEOUT, TEMPORARY)) {
                try {
                    if (!errorList.isEmpty()) {
                        errorList.clear();
                    }
                    deleteResource(transaction, path, errorList, req, resp);
                    if (!errorList.isEmpty()) {
                        sendReport(req, resp, errorList);
                    }
                } catch (final AccessDeniedException e) {
                    resp.sendError(WebdavStatus.SC_FORBIDDEN);
                } catch (final ObjectAlreadyExistsException e) {
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
     * deletes the recources at "path"
     * 
     * @param transaction
     *            indicates that the method is within the scope of a WebDAV transaction
     * @param path
     *            the folder to be deleted
     * @param errorList
     *            all errors that ocurred
     * @param req
     *            HttpServletRequest
     * @param resp
     *            HttpServletResponse
     * @throws WebdavException
     *             if an error in the underlying store occurs
     * @throws IOException
     *             when an error occurs while sending the response
     */
    public void deleteResource(final ITransaction transaction, final String path,
            final Map<String, WebdavStatus> errorList, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, WebdavException {

        resp.setStatus(WebdavStatus.SC_NO_CONTENT);

        if (!this.readOnly) {

            StoredObject so = this.store.getStoredObject(transaction, path);
            if (so != null) {

                if (so.isResource()) {
                    this.store.removeObject(transaction, path);
                } else {
                    if (so.isFolder()) {
                        deleteFolder(transaction, path, errorList, req, resp);
                        this.store.removeObject(transaction, path);
                    } else {
                        resp.sendError(WebdavStatus.SC_NOT_FOUND);
                    }
                }
            } else {
                resp.sendError(WebdavStatus.SC_NOT_FOUND);
            }
            so = null;

        } else {
            resp.sendError(WebdavStatus.SC_FORBIDDEN);
        }
    }

    /**
     * 
     * helper method of deleteResource() deletes the folder and all of its contents
     * 
     * @param transaction
     *            indicates that the method is within the scope of a WebDAV transaction
     * @param path
     *            the folder to be deleted
     * @param errorList
     *            all errors that ocurred
     * @param req
     *            HttpServletRequest
     * @param resp
     *            HttpServletResponse
     * @throws WebdavException
     *             if an error in the underlying store occurs
     */
    private void deleteFolder(final ITransaction transaction, final String path,
            final Map<String, WebdavStatus> errorList, final IWebdavRequest req, final IWebdavResponse resp)
            throws WebdavException {

        String[] children = this.store.getChildrenNames(transaction, path);
        children = children == null ? new String[] {} : children;
        StoredObject so = null;
        for (int i = children.length - 1; i >= 0; i--) {
            children[i] = "/" + children[i];
            try {
                so = this.store.getStoredObject(transaction, path + children[i]);
                if (so.isResource()) {
                    this.store.removeObject(transaction, path + children[i]);

                } else {
                    deleteFolder(transaction, path + children[i], errorList, req, resp);

                    this.store.removeObject(transaction, path + children[i]);

                }
            } catch (final AccessDeniedException e) {
                errorList.put(path + children[i], SC_FORBIDDEN);
            } catch (final ObjectNotFoundException e) {
                errorList.put(path + children[i], SC_NOT_FOUND);
            } catch (final WebdavException e) {
                errorList.put(path + children[i], SC_INTERNAL_SERVER_ERROR);
            }
        }
        so = null;

    }

}
