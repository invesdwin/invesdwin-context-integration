package de.invesdwin.context.integration.ftp.server.test;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.ftp.server.FtpServerContextLocation;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.ITestContext;
import de.invesdwin.context.test.stub.StubSupport;
import jakarta.inject.Named;

@Named
@Immutable
public class FtpServerContextLocationStub extends StubSupport {

    @Override
    public void tearDownOnce(final ATest test, final ITestContext ctx) {
        if (!ctx.isFinishedGlobal()) {
            return;
        }
        FtpServerContextLocation.deactivate();
    }

}
