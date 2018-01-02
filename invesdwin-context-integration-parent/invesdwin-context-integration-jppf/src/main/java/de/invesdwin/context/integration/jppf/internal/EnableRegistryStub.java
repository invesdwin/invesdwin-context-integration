package de.invesdwin.context.integration.jppf.internal;

import java.util.List;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.ws.registry.RegistryServiceStub;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.stub.StubSupport;

@Immutable
@Named
public class EnableRegistryStub extends StubSupport {

    @Override
    public void setUpContextLocations(final ATest test, final List<PositionedResource> locations) throws Exception {
        //allow download of financial data
        RegistryServiceStub.setEnabled(false);
    }

}
