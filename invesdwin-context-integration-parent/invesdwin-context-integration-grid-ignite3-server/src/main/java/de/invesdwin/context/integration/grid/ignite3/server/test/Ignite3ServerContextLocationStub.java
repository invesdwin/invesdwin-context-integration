package de.invesdwin.context.integration.grid.ignite3.server.test;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.grid.ignite3.server.Ignite3ServerContextLocation;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.ITestContext;
import de.invesdwin.context.test.stub.StubSupport;
import jakarta.inject.Named;

@Named
@Immutable
public class Ignite3ServerContextLocationStub extends StubSupport {

    @Override
    public void tearDownOnce(final ATest test, final ITestContext ctx) {
        if (!ctx.isFinishedGlobal()) {
            return;
        }
        Ignite3ServerContextLocation.deactivate();
    }

}
