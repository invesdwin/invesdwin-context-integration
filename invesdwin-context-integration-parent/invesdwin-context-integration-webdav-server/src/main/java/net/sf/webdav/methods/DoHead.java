package net.sf.webdav.methods;

import static net.sf.webdav.WebdavStatus.SC_NOT_FOUND;
import static net.sf.webdav.WebdavStatus.SC_NOT_MODIFIED;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import net.sf.webdav.StoredObject;
import net.sf.webdav.WebdavStatus;
import net.sf.webdav.exceptions.AccessDeniedException;
import net.sf.webdav.exceptions.ObjectAlreadyExistsException;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.locking.ResourceLocks;
import net.sf.webdav.spi.IMimeTyper;
import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;
import net.sf.webdav.spi.IWebdavStore;

@NotThreadSafe
public class DoHead extends AWebdavMethod {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoHead.class);

    protected String dftIndexFile;

    protected IWebdavStore store;

    protected String insteadOf404;

    protected ResourceLocks resourceLocks;

    protected IMimeTyper mimeTyper;

    protected boolean contentLength;

    public DoHead(final IWebdavStore store, final String dftIndexFile, final String insteadOf404,
            final ResourceLocks resourceLocks, final IMimeTyper mimeTyper, final boolean contentLengthHeader) {
        this.store = store;
        this.dftIndexFile = dftIndexFile;
        this.insteadOf404 = insteadOf404;
        this.resourceLocks = resourceLocks;
        this.mimeTyper = mimeTyper;
        this.contentLength = contentLengthHeader;
    }

    @Override
    public void execute(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException, WebdavException {

        // determines if the uri exists.

        boolean bUriExists = false;

        String path = getRelativePath(req);
        LOG.trace("-- " + this.getClass().getName());

        StoredObject so = this.store.getStoredObject(transaction, path);
        if (so == null) {
            if (this.insteadOf404 != null && !this.insteadOf404.trim().equals("")) {
                path = this.insteadOf404;
                so = this.store.getStoredObject(transaction, this.insteadOf404);
            }
        } else {
            bUriExists = true;
        }

        if (so != null) {
            if (so.isFolder()) {
                if (this.dftIndexFile != null && !this.dftIndexFile.trim().equals("")) {
                    resp.sendRedirect(resp.encodeRedirectURL(req.getRequestURI() + this.dftIndexFile));
                    return;
                }
            } else if (so.isNullResource()) {
                final String methodsAllowed = ADeterminableMethod.determineMethodsAllowed(so);
                resp.addHeader("Allow", methodsAllowed);
                resp.sendError(WebdavStatus.SC_METHOD_NOT_ALLOWED);
                return;
            }

            //CHECKSTYLE:OFF
            final String tempLockOwner = "doGet" + System.currentTimeMillis() + req.toString();
            //CHECKSTYLE:ON

            if (this.resourceLocks.lock(transaction, path, tempLockOwner, false, 0, TEMP_TIMEOUT, TEMPORARY)) {
                try {

                    final String eTagMatch = req.getHeader("If-None-Match");
                    if (eTagMatch != null) {
                        if (eTagMatch.equals(getETag(so))) {
                            resp.setStatus(SC_NOT_MODIFIED);
                            return;
                        }
                    }

                    executeLocked(transaction, req, resp, path, so);
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
            folderBody(transaction, path, resp, req);
        }

        if (!bUriExists) {
            resp.setStatus(WebdavStatus.SC_NOT_FOUND);
        }

    }

    private void executeLocked(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp,
            final String path, final StoredObject so) throws IOException, WebdavException {
        if (so.isResource()) {
            // path points to a file but ends with / or \
            if (path.endsWith("/") || (path.endsWith("\\"))) {
                resp.sendError(SC_NOT_FOUND, req.getRequestURI());
            } else {

                // setting headers
                final long lastModified = so.getLastModified().getTime();
                resp.setDateHeader("last-modified", lastModified);

                final String eTag = getETag(so);
                resp.addHeader("ETag", eTag);

                final long resourceLength = so.getResourceLength();

                if (this.contentLength && resourceLength > 0) {
                    if (resourceLength <= Integer.MAX_VALUE) {
                        resp.setContentLength((int) resourceLength);
                    } else {
                        resp.setHeader("content-length", "" + resourceLength);
                        // is "content-length" the right header?
                        // is long a valid format?
                    }
                }

                final String mimeType = this.mimeTyper.getMimeType(path);
                if (mimeType != null) {
                    resp.setContentType(mimeType);
                } else {
                    final int lastSlash = path.replace('\\', '/').lastIndexOf('/');
                    final int lastDot = path.indexOf(".", lastSlash);
                    if (lastDot == -1) {
                        resp.setContentType("text/html");
                    }
                }

                doBody(transaction, resp, path);
            }
        } else {
            folderBody(transaction, path, resp, req);
        }
    }

    protected void folderBody(final ITransaction transaction, final String path, final IWebdavResponse resp,
            final IWebdavRequest req) throws IOException, WebdavException {
        // no body for HEAD
    }

    protected void doBody(final ITransaction transaction, final IWebdavResponse resp, final String path)
            throws IOException, WebdavException {
        // no body for HEAD
    }
}
