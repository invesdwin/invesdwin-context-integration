package net.sf.webdav.methods;

import java.io.IOException;

import net.sf.webdav.exceptions.LockFailedException;
import net.sf.webdav.exceptions.WebdavException;
import net.sf.webdav.spi.ITransaction;
import net.sf.webdav.spi.IWebdavRequest;
import net.sf.webdav.spi.IWebdavResponse;

public interface IWebdavMethod {

    void execute(ITransaction transaction, IWebdavRequest req, IWebdavResponse resp)
            throws IOException, LockFailedException, WebdavException;

}
