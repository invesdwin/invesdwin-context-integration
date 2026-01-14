package de.invesdwin.context.integration.ws.registry;

import javax.annotation.concurrent.ThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.ITestContextSetup;
import de.invesdwin.util.assertions.Assertions;
import jakarta.inject.Inject;

@ThreadSafe
public class RegistryDestinationProviderTest extends ATest {

    @Inject
    private RegistryDestinationProvider destinationProvider;

    @Override
    public void setUpContext(final ITestContextSetup ctx) throws Exception {
        super.setUpContext(ctx);
        ctx.deactivateBean(RegistryServiceStub.class);
        ctx.activateBean(RegistryDestinationProvider.class);
    }

    @Test
    public void testGetDestination() {
        destinationProvider.setServiceName("webproxy.broker");
        Assertions.assertThat(destinationProvider.getDestination()).isNotNull();
    }

}
