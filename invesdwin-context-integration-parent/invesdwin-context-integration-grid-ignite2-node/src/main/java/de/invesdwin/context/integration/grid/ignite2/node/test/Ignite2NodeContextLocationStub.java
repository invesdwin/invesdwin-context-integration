package de.invesdwin.context.integration.grid.ignite2.node.test;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.grid.ignite2.node.Ignite2NodeContextLocation;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.ITestContext;
import de.invesdwin.context.test.stub.StubSupport;
import jakarta.inject.Named;

@Named
@Immutable
public class Ignite2NodeContextLocationStub extends StubSupport {

    @Override
    public void tearDownOnce(final ATest test, final ITestContext ctx) {
        if (!ctx.isFinishedGlobal()) {
            return;
        }
        Ignite2NodeContextLocation.deactivate();
    }

}
