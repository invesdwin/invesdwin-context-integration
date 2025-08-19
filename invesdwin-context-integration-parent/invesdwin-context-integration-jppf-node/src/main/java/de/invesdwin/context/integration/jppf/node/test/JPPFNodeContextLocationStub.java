package de.invesdwin.context.integration.jppf.node.test;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.jppf.node.JPPFNodeContextLocation;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.context.test.stub.StubSupport;
import jakarta.inject.Named;

@Named
@Immutable
public class JPPFNodeContextLocationStub extends StubSupport {

    @Override
    public void tearDownOnce(final ATest test, final TestContext ctx) {
        if (!ctx.isFinishedGlobal()) {
            return;
        }
        JPPFNodeContextLocation.deactivate();
    }

}
