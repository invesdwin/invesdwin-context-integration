package de.invesdwin.context.integration.ws.registry;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.ws.registry.internal.RegistryServiceStubImpl;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.ITestContext;
import de.invesdwin.context.test.ITestContextSetup;
import de.invesdwin.context.test.stub.StubSupport;
import jakarta.inject.Named;

@Named
@ThreadSafe
public class RegistryServiceStub extends StubSupport {

    @Override
    public void setUpContext(final ATest test, final ITestContextSetup ctx) {
        if (isEnabled()) {
            ctx.replaceBean(IRegistryService.class, RegistryServiceStubImpl.class);
        }
    }

    @Override
    public void tearDownOnce(final ATest test, final ITestContext ctx) {
        if (!ctx.isFinishedGlobal()) {
            return;
        }
        setEnabled(true);
    }

    public static void setEnabled(final boolean enabled) {
        RegistryServiceStubImpl.setEnabled(enabled);
    }

    public static boolean isEnabled() {
        return RegistryServiceStubImpl.isEnabled();
    }

    public static void override(final String serviceName, final URI accessURI) {
        RegistryServiceStubImpl.override(serviceName, accessURI);
    }

}
