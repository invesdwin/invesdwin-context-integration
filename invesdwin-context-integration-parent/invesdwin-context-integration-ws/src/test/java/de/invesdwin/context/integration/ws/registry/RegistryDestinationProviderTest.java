package de.invesdwin.context.integration.ws.registry;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import org.junit.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.util.assertions.Assertions;

@ThreadSafe
public class RegistryDestinationProviderTest extends ATest {

    @Inject
    private RegistryDestinationProvider destinationProvider;

    @Override
    public void setUpContext(final TestContext ctx) throws Exception {
        super.setUpContext(ctx);
        ctx.deactivate(RegistryServiceStub.class);
        ctx.activate(RegistryDestinationProvider.class);
    }

    @Test
    public void testGetDestination() {
        destinationProvider.setServiceName("webproxy.broker");
        Assertions.assertThat(destinationProvider.getDestination()).isNotNull();
    }

}
