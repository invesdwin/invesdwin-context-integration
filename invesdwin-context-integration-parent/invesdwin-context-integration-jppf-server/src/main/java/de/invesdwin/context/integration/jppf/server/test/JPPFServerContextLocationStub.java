package de.invesdwin.context.integration.jppf.server.test;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import de.invesdwin.context.integration.jppf.server.ConfiguredJPPFServer;
import de.invesdwin.context.integration.jppf.server.JPPFServerContextLocation;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.stub.StubSupport;

@Named
@Immutable
public class JPPFServerContextLocationStub extends StubSupport {

    @Override
    public void tearDownOnce(final ATest test) {
        JPPFServerContextLocation.deactivate();
        ConfiguredJPPFServer.setNodeStartupEnabled(null);
    }

}
