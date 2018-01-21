package de.invesdwin.context.integration.ftp.server.test;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import de.invesdwin.context.integration.ftp.server.FtpServerContextLocation;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.stub.StubSupport;

@Named
@Immutable
public class FtpServerContextLocationStub extends StubSupport {

    @Override
    public void tearDownOnce(final ATest test) {
        FtpServerContextLocation.deactivate();
    }

}
