package net.sf.webdav.methods;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import net.sf.webdav.StoredObject;
import net.sf.webdav.WebdavStatus;
import net.sf.webdav.exceptions.AccessDeniedException;
import net.sf.webdav.exceptions.LockFailedException;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.locking.ResourceLocks;
import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;
import net.sf.webdav.spi.IWebdavStore;

@NotThreadSafe
public class DoOptions extends ADeterminableMethod {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoOptions.class);

    private final IWebdavStore store;
    private final ResourceLocks resourceLocks;

    public DoOptions(final IWebdavStore store, final ResourceLocks resLocks) {
        this.store = store;
        this.resourceLocks = resLocks;
    }

    @Override
    public void execute(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, LockFailedException {

        LOG.trace("-- " + this.getClass().getName());

        //CHECKSTYLE:OFF
        final String tempLockOwner = "doOptions" + System.currentTimeMillis() + req.toString();
        //CHECKSTYLE:ON
        final String path = getRelativePath(req);
        if (this.resourceLocks.lock(transaction, path, tempLockOwner, false, 0, TEMP_TIMEOUT, TEMPORARY)) {
            StoredObject so = null;
            try {
                resp.addHeader("DAV", "1, 2");

                so = this.store.getStoredObject(transaction, path);
                final String methodsAllowed = determineMethodsAllowed(so);
                resp.addHeader("Allow", methodsAllowed);
                resp.addHeader("MS-Author-Via", "DAV");
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
    }
}
