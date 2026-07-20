package net.sf.webdav;

import java.io.IOException;
import java.security.Principal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.error.Throwables;
import net.sf.webdav.exceptions.UnauthenticatedException;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.locking.ResourceLocks;
import net.sf.webdav.methods.DoCopy;
import net.sf.webdav.methods.DoDelete;
import net.sf.webdav.methods.DoGet;
import net.sf.webdav.methods.DoHead;
import net.sf.webdav.methods.DoLock;
import net.sf.webdav.methods.DoMkcol;
import net.sf.webdav.methods.DoMove;
import net.sf.webdav.methods.DoNotImplemented;
import net.sf.webdav.methods.DoOptions;
import net.sf.webdav.methods.DoPropfind;
import net.sf.webdav.methods.DoProppatch;
import net.sf.webdav.methods.DoPut;
import net.sf.webdav.methods.DoUnlock;
import net.sf.webdav.methods.IWebdavMethod;
import net.sf.webdav.spi.IMimeTyper;
import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavConfig;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;
import net.sf.webdav.spi.IWebdavStore;

/**
 * Adapted from https://github.com/Commonjava/webdav-handler (https://github.com/subes/webdav-handler)
 */
@NotThreadSafe
public class WebdavService {
    private static final boolean READ_ONLY = false;

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(WebdavService.class);

    private final ResourceLocks resLocks;

    private final IWebdavStore store;

    //CHECKSTYLE:OFF
    private final Map<String, IWebdavMethod> methodMap = new LinkedHashMap<String, IWebdavMethod>();
    //CHECKSTYLE:ON

    public WebdavService(final IWebdavConfig config, final IWebdavStore store, final IMimeTyper mimeTyper) {
        this.store = store;
        this.resLocks = new ResourceLocks();

        final boolean lazyFolderCreationOnPut = config.isLazyFolderCreationOnPut();

        final String dftIndexFile = config.getDefaultIndexPath();
        final String insteadOf404 = config.getAlt404Path();

        final boolean noContentLengthHeader = config.isOmitContentLengthHeaders();

        register("GET", new DoGet(store, dftIndexFile, insteadOf404, this.resLocks, mimeTyper, !noContentLengthHeader));
        register("HEAD",
                new DoHead(store, dftIndexFile, insteadOf404, this.resLocks, mimeTyper, !noContentLengthHeader));
        final DoDelete doDelete = (DoDelete) register("DELETE", new DoDelete(store, this.resLocks, READ_ONLY));
        final DoCopy doCopy = (DoCopy) register("COPY", new DoCopy(store, this.resLocks, doDelete, READ_ONLY));
        register("LOCK", new DoLock(store, this.resLocks, READ_ONLY));
        register("UNLOCK", new DoUnlock(store, this.resLocks, READ_ONLY));
        register("MOVE", new DoMove(this.resLocks, doDelete, doCopy, READ_ONLY));
        register("MKCOL", new DoMkcol(store, this.resLocks, READ_ONLY));
        register("OPTIONS", new DoOptions(store, this.resLocks));
        register("PUT", new DoPut(store, this.resLocks, READ_ONLY, lazyFolderCreationOnPut));
        register("PROPFIND", new DoPropfind(store, this.resLocks, mimeTyper));
        register("PROPPATCH", new DoProppatch(store, this.resLocks, READ_ONLY));
        register("*NO*IMPL*", new DoNotImplemented(READ_ONLY));
    }

    private IWebdavMethod register(final String methodName, final IWebdavMethod method) {
        this.methodMap.put(methodName, method);
        return method;
    }

    /**
     * Handles the special WebDAV methods.
     */
    public void service(final IWebdavRequest req, final IWebdavResponse resp) throws WebdavException, IOException {

        final String methodName = req.getMethod();
        ITransaction transaction = null;
        boolean needRollback = false;

        if (LOG.isTraceEnabled()) {
            debugRequest(methodName, req);
        }

        try {
            final Principal userPrincipal = req.getUserPrincipal();
            transaction = store.begin(userPrincipal);
            needRollback = true;
            store.checkAuthentication(transaction);
            resp.setStatus(WebdavStatus.SC_OK);

            IWebdavMethod methodExecutor = null;
            try {
                methodExecutor = this.methodMap.get(methodName);
                if (methodExecutor == null) {
                    methodExecutor = this.methodMap.get("*NO*IMPL*");
                }

                LOG.info("Executing: " + methodExecutor.getClass().getSimpleName());
                methodExecutor.execute(transaction, req, resp);

                store.commit(transaction);
                needRollback = false;
            } catch (final IOException e) {
                LOG.error("IOException: " + Throwables.getFullStackTrace(e));
                resp.sendError(WebdavStatus.SC_INTERNAL_SERVER_ERROR);
                store.rollback(transaction);
                throw new WebdavException("I/O error executing %s: %s", e, methodExecutor.getClass().getSimpleName(),
                        e.getMessage());
            }

        } catch (final UnauthenticatedException e) {
            resp.sendError(WebdavStatus.SC_FORBIDDEN);
        } catch (final WebdavException e) {
            LOG.error("WebdavException: " + Throwables.getFullStackTrace(e));
            throw e;
        } catch (final Exception e) {
            LOG.error("Exception: " + Throwables.getFullStackTrace(e));
        } finally {
            if (needRollback) {
                store.rollback(transaction);
            }
        }

    }

    private void debugRequest(final String methodName, final IWebdavRequest req) {
        LOG.trace("-----------");
        LOG.trace("WebdavServlet\n request: methodName = " + methodName);
        //CHECKSTYLE:OFF
        LOG.trace("time: " + System.currentTimeMillis());
        //CHECKSTYLE:ON
        LOG.trace("path: " + req.getRequestURI());
        LOG.trace("-----------");
        Set<String> e = req.getHeaderNames();
        if (e != null) {
            for (final String s : e) {
                LOG.trace("header: " + s + " " + req.getHeader(s));
            }
        }
        e = req.getAttributeNames();
        if (e != null) {
            for (final String s : e) {
                LOG.trace("attribute: " + s + " " + req.getAttribute(s));
            }
        }
        e = req.getParameterNames();
        if (e != null) {
            for (final String s : e) {
                LOG.trace("parameter: " + s + " " + req.getParameter(s));
            }
        }
    }

}
