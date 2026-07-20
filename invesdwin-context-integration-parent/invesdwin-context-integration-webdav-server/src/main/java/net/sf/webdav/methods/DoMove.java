package net.sf.webdav.methods;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import net.sf.webdav.WebdavStatus;
import net.sf.webdav.exceptions.AccessDeniedException;
import net.sf.webdav.exceptions.LockFailedException;
import net.sf.webdav.exceptions.ObjectAlreadyExistsException;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.locking.ResourceLocks;
import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;

@NotThreadSafe
public class DoMove extends AWebdavMethod {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoMove.class);

    private final ResourceLocks resourceLocks;

    private final DoDelete doDelete;

    private final DoCopy doCopy;

    private final boolean readOnly;

    public DoMove(final ResourceLocks resourceLocks, final DoDelete doDelete, final DoCopy doCopy,
            final boolean readOnly) {
        this.resourceLocks = resourceLocks;
        this.doDelete = doDelete;
        this.doCopy = doCopy;
        this.readOnly = readOnly;
    }

    @Override
    public void execute(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, LockFailedException {

        if (!this.readOnly) {
            LOG.trace("-- " + this.getClass().getName());

            final String sourcePath = getRelativePath(req);
            //CHECKSTYLE:OFF
            final Map<String, WebdavStatus> errorList = new LinkedHashMap<String, WebdavStatus>();
            //CHECKSTYLE:ON

            if (!checkLocks(transaction, req, resp, this.resourceLocks, sourcePath)) {
                errorList.put(sourcePath, WebdavStatus.SC_LOCKED);
                sendReport(req, resp, errorList);
                return;
            }

            final String destinationPath = req.getHeader("Destination");
            if (destinationPath == null) {
                resp.sendError(WebdavStatus.SC_BAD_REQUEST);
                return;
            }

            if (!checkLocks(transaction, req, resp, this.resourceLocks, destinationPath)) {
                errorList.put(destinationPath, WebdavStatus.SC_LOCKED);
                sendReport(req, resp, errorList);
                return;
            }

            //CHECKSTYLE:OFF
            final String tempLockOwner = "doMove" + System.currentTimeMillis() + req.toString();
            //CHECKSTYLE:ON

            if (this.resourceLocks.lock(transaction, sourcePath, tempLockOwner, false, 0, TEMP_TIMEOUT, TEMPORARY)) {
                try {

                    if (this.doCopy.copyResource(transaction, req, resp)) {
                        if (!errorList.isEmpty()) {
                            errorList.clear();
                        }
                        this.doDelete.deleteResource(transaction, sourcePath, errorList, req, resp);
                        if (!errorList.isEmpty()) {
                            sendReport(req, resp, errorList);
                        }
                    }

                } catch (final AccessDeniedException e) {
                    resp.sendError(WebdavStatus.SC_FORBIDDEN);
                } catch (final ObjectAlreadyExistsException e) {
                    resp.sendError(WebdavStatus.SC_NOT_FOUND, req.getRequestURI());
                } catch (final WebdavException e) {
                    resp.sendError(WebdavStatus.SC_INTERNAL_SERVER_ERROR);
                } finally {
                    this.resourceLocks.unlockTemporaryLockedObjects(transaction, sourcePath, tempLockOwner);
                }
            } else {
                errorList.put(req.getHeader("Destination"), WebdavStatus.SC_LOCKED);
                sendReport(req, resp, errorList);
            }
        } else {
            resp.sendError(WebdavStatus.SC_FORBIDDEN);

        }

    }

}
