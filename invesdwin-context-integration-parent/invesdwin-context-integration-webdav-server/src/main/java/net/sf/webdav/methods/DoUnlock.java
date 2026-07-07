package net.sf.webdav.methods;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.error.Throwables;
import net.sf.webdav.StoredObject;
import net.sf.webdav.WebdavStatus;
import net.sf.webdav.exceptions.LockFailedException;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.locking.IResourceLocks;
import net.sf.webdav.locking.LockedObject;
import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;
import net.sf.webdav.spi.IWebdavStore;

@NotThreadSafe
public class DoUnlock extends ADeterminableMethod {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoUnlock.class);

    private final IWebdavStore store;

    private final IResourceLocks resourceLocks;

    private final boolean readOnly;

    public DoUnlock(final IWebdavStore store, final IResourceLocks resourceLocks, final boolean readOnly) {
        this.store = store;
        this.resourceLocks = resourceLocks;
        this.readOnly = readOnly;
    }

    @Override
    public void execute(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, WebdavException {
        LOG.trace("-- " + this.getClass().getName());

        if (this.readOnly) {
            resp.sendError(WebdavStatus.SC_FORBIDDEN);
            return;
        } else {

            final String path = getRelativePath(req);
            //CHECKSTYLE:OFF
            final String tempLockOwner = "doUnlock" + System.currentTimeMillis() + req.toString();
            //CHECKSTYLE:ON
            try {
                if (this.resourceLocks.lock(transaction, path, tempLockOwner, false, 0, TEMP_TIMEOUT, TEMPORARY)) {
                    executeLocked(transaction, req, resp, path);
                }
            } catch (final LockFailedException e) {
                LOG.warn(Throwables.getFullStackTrace(e));
            } finally {
                this.resourceLocks.unlockTemporaryLockedObjects(transaction, path, tempLockOwner);
            }
        }
    }

    private void executeLocked(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp,
            final String path) throws WebdavException, IOException {
        final String lockId = getLockIdFromLockTokenHeader(req);
        final LockedObject lo;
        //CHECKSTYLE:OFF
        if (lockId != null && ((lo = this.resourceLocks.getLockedObjectByID(transaction, lockId)) != null)) {
            //CHECKSTYLE:ON

            final String[] owners = lo.getOwner();
            String owner = null;
            if (lo.isShared()) {
                // more than one owner is possible
                if (owners != null) {
                    for (final String owner2 : owners) {
                        // remove owner from LockedObject
                        lo.removeLockedObjectOwner(owner2);
                    }
                }
            } else {
                // exclusive, only one lock owner
                if (owners != null) {
                    owner = owners[0];
                } else {
                    owner = null;
                }
            }

            if (this.resourceLocks.unlock(transaction, lockId, owner)) {
                final StoredObject so = this.store.getStoredObject(transaction, path);
                if (so.isNullResource()) {
                    this.store.removeObject(transaction, path);
                }

                resp.setStatus(WebdavStatus.SC_NO_CONTENT);
            } else {
                LOG.trace("DoUnlock failure at " + lo.getPath());
                resp.sendError(WebdavStatus.SC_METHOD_FAILURE);
            }

        } else {
            resp.sendError(WebdavStatus.SC_BAD_REQUEST);
        }
    }

}
