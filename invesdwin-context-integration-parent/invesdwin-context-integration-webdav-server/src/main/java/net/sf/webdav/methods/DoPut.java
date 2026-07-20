package net.sf.webdav.methods;

import static net.sf.webdav.WebdavStatus.SC_BAD_REQUEST;
import static net.sf.webdav.WebdavStatus.SC_CREATED;
import static net.sf.webdav.WebdavStatus.SC_FORBIDDEN;
import static net.sf.webdav.WebdavStatus.SC_INTERNAL_SERVER_ERROR;
import static net.sf.webdav.WebdavStatus.SC_LOCKED;
import static net.sf.webdav.WebdavStatus.SC_NOT_FOUND;
import static net.sf.webdav.WebdavStatus.SC_NO_CONTENT;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import net.sf.webdav.StoredObject;
import net.sf.webdav.WebdavStatus;
import net.sf.webdav.exceptions.AccessDeniedException;
import net.sf.webdav.exceptions.LockFailedException;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.locking.IResourceLocks;
import net.sf.webdav.locking.LockedObject;
import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;
import net.sf.webdav.spi.IWebdavStore;

@NotThreadSafe
public class DoPut extends AWebdavMethod {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoPut.class);

    private final IWebdavStore store;

    private final IResourceLocks resourceLocks;

    private final boolean readOnly;

    private final boolean lazyFolderCreationOnPut;

    private String userAgent;

    public DoPut(final IWebdavStore store, final IResourceLocks resLocks, final boolean readOnly,
            final boolean lazyFolderCreationOnPut) {
        this.store = store;
        this.resourceLocks = resLocks;
        this.readOnly = readOnly;
        this.lazyFolderCreationOnPut = lazyFolderCreationOnPut;
    }

    @Override
    public void execute(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, LockFailedException {
        LOG.trace("-- " + this.getClass().getName());

        if (!this.readOnly) {
            final String path = getRelativePath(req);
            final String parentPath = getParentPath(path);

            this.userAgent = req.getHeader("User-Agent");

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
            final String tempLockOwner = "doPut" + System.currentTimeMillis() + req.toString();
            //CHECKSTYLE:ON
            if (this.resourceLocks.lock(transaction, path, tempLockOwner, false, 0, TEMP_TIMEOUT, TEMPORARY)) {
                try {
                    executeLocked(transaction, req, resp, path, parentPath, errorList);
                } catch (final AccessDeniedException e) {
                    resp.sendError(SC_FORBIDDEN);
                } catch (final WebdavException e) {
                    resp.sendError(SC_INTERNAL_SERVER_ERROR);
                } finally {
                    this.resourceLocks.unlockTemporaryLockedObjects(transaction, path, tempLockOwner);
                }
            } else {
                resp.sendError(SC_INTERNAL_SERVER_ERROR);
            }
        } else {
            resp.sendError(SC_FORBIDDEN);
        }

    }

    private void executeLocked(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp,
            final String path, final String parentPath, final Map<String, WebdavStatus> errorList)
            throws WebdavException, IOException {
        final StoredObject parentSo = this.store.getStoredObject(transaction, parentPath);
        if (parentPath != null && parentSo != null && parentSo.isResource()) {
            resp.sendError(SC_FORBIDDEN);
            return;

        } else if (parentPath != null && parentSo == null && this.lazyFolderCreationOnPut) {
            this.store.createFolder(transaction, parentPath);

        } else if (parentPath != null && parentSo == null && !this.lazyFolderCreationOnPut) {
            errorList.put(parentPath, SC_NOT_FOUND);
            sendReport(req, resp, errorList);
            return;
        }

        StoredObject so = this.store.getStoredObject(transaction, path);

        if (so == null) {
            this.store.createResource(transaction, path);
            // resp.setStatus(SC_CREATED);
        } else {
            // This has already been created, just update the data
            if (so.isNullResource()) {

                final LockedObject nullResourceLo = this.resourceLocks.getLockedObjectByPath(transaction, path);
                if (nullResourceLo == null) {
                    resp.sendError(SC_INTERNAL_SERVER_ERROR);
                    return;
                }
                final String nullResourceLockToken = nullResourceLo.getID();
                final String[] lockTokens = getLockIdFromIfHeader(req);
                String lockToken = null;
                if (lockTokens != null) {
                    lockToken = lockTokens[0];
                } else {
                    resp.sendError(SC_BAD_REQUEST);
                    return;
                }
                if (lockToken.equals(nullResourceLockToken)) {
                    so.setNullResource(false);
                    so.setFolder(false);

                    final String[] nullResourceLockOwners = nullResourceLo.getOwner();
                    String owner = null;
                    if (nullResourceLockOwners != null) {
                        owner = nullResourceLockOwners[0];
                    }

                    if (!this.resourceLocks.unlock(transaction, lockToken, owner)) {
                        resp.sendError(SC_INTERNAL_SERVER_ERROR);
                    }
                } else {
                    errorList.put(path, SC_LOCKED);
                    sendReport(req, resp, errorList);
                }
            }
        }
        // User-Agent workarounds
        doUserAgentWorkaround(resp);

        // setting resourceContent
        final long resourceLength = this.store.setResourceContent(transaction, path, req.getInputStream(),
                req.getContentLength());

        so = this.store.getStoredObject(transaction, path);
        if (resourceLength != -1) {
            so.setResourceLength(resourceLength);
            // Now lets report back what was actually saved
        }
    }

    /**
     * @param resp
     */
    private void doUserAgentWorkaround(final IWebdavResponse resp) {
        if (this.userAgent != null && this.userAgent.indexOf("WebDAVFS") != -1
                && this.userAgent.indexOf("Transmit") == -1) {
            LOG.trace("DoPut.execute() : do workaround for user agent '" + this.userAgent + "'");
            resp.setStatus(SC_CREATED);
        } else if (this.userAgent != null && this.userAgent.indexOf("Transmit") != -1) {
            // Transmit also uses WEBDAVFS 1.x.x but crashes
            // with SC_CREATED response
            LOG.trace("DoPut.execute() : do workaround for user agent '" + this.userAgent + "'");
            resp.setStatus(SC_NO_CONTENT);
        } else {
            resp.setStatus(SC_CREATED);
        }
    }
}
