package net.sf.webdav.methods;

import static net.sf.webdav.WebdavStatus.SC_FORBIDDEN;
import static net.sf.webdav.WebdavStatus.SC_NOT_IMPLEMENTED;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;

@NotThreadSafe
public class DoNotImplemented implements IWebdavMethod {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DoNotImplemented.class);

    private final boolean readOnly;

    public DoNotImplemented(final boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public void execute(final ITransaction transaction, final IWebdavRequest req, final IWebdavResponse resp)
            throws IOException {
        LOG.trace("-- " + req.getMethod());

        if (this.readOnly) {
            resp.sendError(SC_FORBIDDEN);
        } else {
            resp.sendError(SC_NOT_IMPLEMENTED);
        }
    }
}
