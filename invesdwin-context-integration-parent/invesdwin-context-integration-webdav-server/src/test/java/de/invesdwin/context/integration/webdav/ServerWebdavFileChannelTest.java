package de.invesdwin.context.integration.webdav;

import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.webdav.server.WebdavServerProperties;
import de.invesdwin.context.webserver.test.WebserverTest;

@WebserverTest
@NotThreadSafe
public class ServerWebdavFileChannelTest extends WebdavFileChannelTest {

    @Override
    protected URI getDestination() {
        return WebdavServerProperties.getServerBindUri();
    }

}
