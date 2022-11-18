package de.invesdwin.context.integration.webdav.server.internal;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.webdav.server.WebdavServerProperties;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.impl.LocalFileSystemStore;
import net.sf.webdav.spi.IWebdavStore;

@Immutable
public class ConfiguredWebdavServlet extends WebdavServlet {

    @Override
    protected IWebdavStore initWebdavStore() throws WebdavException {
        return new LocalFileSystemStore(WebdavServerProperties.WORKING_DIRECTORY);
    }

}