package de.invesdwin.context.integration.ftp.server.internal;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

import org.apache.commons.io.FileUtils;

import de.invesdwin.context.ContextDirectoriesStub;
import de.invesdwin.context.integration.ftp.server.FtpServerProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.context.test.stub.StubSupport;

@NotThreadSafe
@Named
public class FtpServerBaseDirectoryStub extends StubSupport {

    static {
        ContextDirectoriesStub.addProtectedDirectory(FtpServerProperties.BASE_DIRECTORY);
    }

    @Override
    public void setUpOnce(final ATest test, final TestContext ctx) throws Exception {
        FileUtils.deleteQuietly(FtpServerProperties.BASE_DIRECTORY);
        FileUtils.forceMkdir(FtpServerProperties.BASE_DIRECTORY);
    }

}
