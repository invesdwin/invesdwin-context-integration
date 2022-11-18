package de.invesdwin.context.integration.jppf.node.test;

import javax.annotation.concurrent.Immutable;
import jakarta.inject.Named;

import de.invesdwin.context.integration.jppf.node.JPPFNodeContextLocation;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.stub.StubSupport;

@Named
@Immutable
public class JPPFNodeContextLocationStub extends StubSupport {

    @Override
    public void tearDownOnce(final ATest test) {
        JPPFNodeContextLocation.deactivate();
    }

}
