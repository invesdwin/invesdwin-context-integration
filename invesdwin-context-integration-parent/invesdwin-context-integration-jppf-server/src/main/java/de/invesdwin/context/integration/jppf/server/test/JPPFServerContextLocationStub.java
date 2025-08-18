package de.invesdwin.context.integration.jppf.server.test;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.jppf.server.ConfiguredJPPFServer;
import de.invesdwin.context.integration.jppf.server.JPPFServerContextLocation;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.context.test.stub.StubSupport;
import jakarta.inject.Named;

@Named
@Immutable
public class JPPFServerContextLocationStub extends StubSupport {

    @Override
    public void tearDownOnce(final ATest test, final TestContext ctx) {
        if (!ctx.isFinished()) {
            return;
        }
        JPPFServerContextLocation.deactivate();
        ConfiguredJPPFServer.setNodeStartupEnabled(null);
    }

}
