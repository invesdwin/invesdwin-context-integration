package net.sf.webdav.methods;

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
public class DoMkcol extends AWebdavMethod {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoMkcol.class);

    private final IWebdavStore store;

    private final IResourceLocks resourceLocks;

    private final boolean readOnly;

    public DoMkcol(final IWebdavStore store, final IResourceLocks resourceLocks, final boolean readOnly) {
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
                // TODO remove
                LOG.trace("MkCol on locked resource (parentPath) not executable!"
                        + "\n Sending SC_FORBIDDEN (403) error response!");

                resp.sendError(WebdavStatus.SC_FORBIDDEN);
                return;
            }

            //CHECKSTYLE:OFF
            final String tempLockOwner = "doMkcol" + System.currentTimeMillis() + req.toString();
            //CHECKSTYLE:ON

            if (this.resourceLocks.lock(transaction, path, tempLockOwner, false, 0, TEMP_TIMEOUT, TEMPORARY)) {
                try {
                    final StoredObject parentSo = this.store.getStoredObject(transaction, parentPath);
                    if (parentSo == null) {
                        // parent not exists
                        resp.sendError(WebdavStatus.SC_CONFLICT);
                        return;
                    }
                    if (parentPath != null && parentSo.isFolder()) {
                        final StoredObject so = this.store.getStoredObject(transaction, path);
                        if (so == null) {
                            this.store.createFolder(transaction, path);
                            resp.setStatus(WebdavStatus.SC_CREATED);
                        } else {
                            executeAlreadyExists(transaction, req, resp, path, errorList, so);
                        }

                    } else if (parentPath != null && parentSo.isResource()) {
                        // TODO remove
                        LOG.trace("MkCol on resource is not executable"
                                + "\n Sending SC_METHOD_NOT_ALLOWED (405) error response!");

                        final String methodsAllowed = ADeterminableMethod.determineMethodsAllowed(parentSo);
                        resp.addHeader("Allow", methodsAllowed);
                        resp.sendError(WebdavStatus.SC_METHOD_NOT_ALLOWED);

                    } else {
                        resp.sendError(WebdavStatus.SC_FORBIDDEN);
                    }
                } catch (final AccessDeniedException e) {
                    resp.sendError(WebdavStatus.SC_FORBIDDEN);
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

    private void executeAlreadyExists(final ITransaction transaction, final IWebdavRequest req,
            final IWebdavResponse resp, final String path, final Map<String, WebdavStatus> errorList,
            final StoredObject so) throws IOException {
        // object already exists
        if (so.isNullResource()) {

            final LockedObject nullResourceLo = this.resourceLocks.getLockedObjectByPath(transaction, path);
            if (nullResourceLo == null) {
                resp.sendError(WebdavStatus.SC_INTERNAL_SERVER_ERROR);
                return;
            }
            final String nullResourceLockToken = nullResourceLo.getID();
            final String[] lockTokens = getLockIdFromIfHeader(req);
            String lockToken = null;
            if (lockTokens != null) {
                lockToken = lockTokens[0];
            } else {
                resp.sendError(WebdavStatus.SC_BAD_REQUEST);
                return;
            }
            if (lockToken.equals(nullResourceLockToken)) {
                so.setNullResource(false);
                so.setFolder(true);

                final String[] nullResourceLockOwners = nullResourceLo.getOwner();
                String owner = null;
                if (nullResourceLockOwners != null) {
                    owner = nullResourceLockOwners[0];
                }

                if (this.resourceLocks.unlock(transaction, lockToken, owner)) {
                    resp.setStatus(WebdavStatus.SC_CREATED);
                } else {
                    resp.sendError(WebdavStatus.SC_INTERNAL_SERVER_ERROR);
                }

            } else {
                // TODO remove
                LOG.trace(
                        "MkCol on lock-null-resource with wrong lock-token!" + "\n Sending multistatus error report!");

                errorList.put(path, WebdavStatus.SC_LOCKED);
                sendReport(req, resp, errorList);
            }

        } else {
            final String methodsAllowed = ADeterminableMethod.determineMethodsAllowed(so);
            resp.addHeader("Allow", methodsAllowed);
            resp.sendError(WebdavStatus.SC_METHOD_NOT_ALLOWED);
        }
    }

}
