package de.invesdwin.context.integration.webdav.server.internal;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.webdav.server.internal.impl.ServletInitWebdavConfig;
import de.invesdwin.context.integration.webdav.server.internal.impl.ServletMimeTyper;
import de.invesdwin.context.integration.webdav.server.internal.impl.ServletWebdavRequest;
import de.invesdwin.context.integration.webdav.server.internal.impl.ServletWebdavResponse;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.sf.webdav.WebdavService;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.impl.LocalFileSystemStore;
import net.sf.webdav.spi.IMimeTyper;
import net.sf.webdav.spi.IWebdavStore;
import net.sf.webdav.spi.WebdavConfig;

/**
 * Adapted from:
 * 
 * <pre>
 *       <dependency>
 *           <groupId>org.commonjava.web</groupId>
 *           <artifactId>webdav-handler-servlet</artifactId>
 *           <version>3.3.0</version>
 *       </dependency>
 * </pre>
 *
 */
@ThreadSafe
public class WebdavServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private WebdavService dav;
    private WebdavConfig davConfig;
    private IWebdavStore store;
    private IMimeTyper mimeTyper;

    @Override
    protected void service(final HttpServletRequest req, final HttpServletResponse resp)
            throws ServletException, IOException {
        try {
            dav.service(new ServletWebdavRequest(req), new ServletWebdavResponse(resp));
        } catch (final WebdavException e) {
            throw new ServletException("Failed to service request: " + e.getMessage(), e);
        }
    }

    @Override
    public void init(final ServletConfig config) throws ServletException {
        super.init(config);
        init();
    }

    @Override
    public void init() throws ServletException {
        if (dav == null) {
            if (store == null) {
                try {
                    store = initWebdavStore();
                } catch (final WebdavException e) {
                    throw new ServletException("Failed to initialize WebDAV store: " + e.getMessage(), e);
                }
            }

            if (davConfig == null) {
                try {
                    davConfig = initWebdavConfig();
                } catch (final WebdavException e) {
                    throw new ServletException("Failed to initialize WebDAV configuration: " + e.getMessage(), e);
                }
            }

            if (mimeTyper == null) {
                try {
                    mimeTyper = initMimeTyper();
                } catch (final WebdavException e) {
                    throw new ServletException("Failed to initialize WebDAV MIME-typer: " + e.getMessage(), e);
                }
            }

            dav = new WebdavService(davConfig, store, mimeTyper);
        }
    }

    protected IMimeTyper initMimeTyper() throws WebdavException {
        return new ServletMimeTyper(getServletConfig().getServletContext());
    }

    protected WebdavConfig initWebdavConfig() throws WebdavException {
        return new ServletInitWebdavConfig(getServletConfig());
    }

    protected IWebdavStore initWebdavStore() throws WebdavException {
        final String rootPath = getServletConfig().getInitParameter(ServletInitWebdavConfig.LOCAL_ROOT_DIR);
        if (rootPath == null) {
            throw new WebdavException("No local filesystem root was specified for WebDAV default store!");
        }

        final File root = new File(rootPath);
        return new LocalFileSystemStore(root);
    }

}
