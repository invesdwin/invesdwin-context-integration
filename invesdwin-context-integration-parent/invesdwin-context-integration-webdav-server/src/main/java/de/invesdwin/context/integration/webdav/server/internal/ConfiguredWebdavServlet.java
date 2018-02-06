package de.invesdwin.context.integration.webdav.server.internal;

import javax.annotation.concurrent.Immutable;
import javax.servlet.ServletException;

import de.invesdwin.context.integration.webdav.server.WebdavServerProperties;
import net.sf.webdav.IWebdavStore;
import net.sf.webdav.LocalFileSystemStore;
import net.sf.webdav.WebDavServletBean;

@Immutable
public class ConfiguredWebdavServlet extends WebDavServletBean {

    @Override
    public void init() throws ServletException {
        final IWebdavStore webdavStore = new LocalFileSystemStore(WebdavServerProperties.WORKING_DIRECTORY);
        super.init(webdavStore, null, null, 1, true);
    }

}