package de.invesdwin.context.integration.ftp;

import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.ftp.server.FtpServerProperties;
import de.invesdwin.context.integration.ftp.server.test.FtpServerTest;
import de.invesdwin.util.lang.uri.URIs;

@FtpServerTest
@NotThreadSafe
public class ServerFtpFileChannelTest extends FtpFileChannelTest {

    @Override
    protected URI getDestination() {
        return URIs.asUri("p://localhost:" + FtpServerProperties.PORT);
    }

}
