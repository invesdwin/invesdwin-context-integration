package de.invesdwin.context.integration.grid.ignite2.server.test;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.grid.ignite2.server.Ignite2ServerContextLocation;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.ITestContext;
import de.invesdwin.context.test.stub.StubSupport;
import jakarta.inject.Named;

@Named
@Immutable
public class Ignite2ServerContextLocationStub extends StubSupport {

    @Override
    public void tearDownOnce(final ATest test, final ITestContext ctx) {
        if (!ctx.isFinishedGlobal()) {
            return;
        }
        Ignite2ServerContextLocation.deactivate();
    }

}
