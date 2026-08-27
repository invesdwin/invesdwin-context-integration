package de.invesdwin.context.integration.grid.ignite2.server.test;

import java.util.List;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.grid.ignite2.server.ConfiguredIgnite2Server;
import de.invesdwin.context.integration.grid.ignite2.server.Ignite2ServerContextLocation;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.ITestContext;
import de.invesdwin.context.test.ITestContextSetup;
import de.invesdwin.context.test.stub.StubSupport;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;
import jakarta.inject.Named;

@Named
@ThreadSafe
public class Ignite2ServerTestStub extends StubSupport {

    @GuardedBy("this.class")
    private static ConfiguredIgnite2Server lastNode;

    static {
        ShutdownHookManager.register(new IShutdownHook() {
            @Override
            public void shutdown() throws Exception {
                maybeStopLastServer();
            }
        });
    }

    @Override
    public void setUpContextLocations(final ATest test, final List<PositionedResource> locations) throws Exception {
        final Ignite2ServerTest annotation = Reflections.getAnnotation(test, Ignite2ServerTest.class);
        if (annotation != null) {
            if (annotation.value()) {
                locations.add(Ignite2ServerContextLocation.SERVER_CONTEXT_LOCATION);
            } else {
                locations.remove(Ignite2ServerContextLocation.SERVER_CONTEXT_LOCATION);
            }
        }
    }

    @Override
    public void setUpContext(final ATest test, final ITestContextSetup ctx) throws Exception {
        if (ctx.isPreMergedContext()) {
            return;
        }
        //if for some reason the tearDownOnce was not executed on the last test (maybe maven killed it?), then try to stop here aswell
        maybeStopLastServer();
    }

    @Override
    public void setUpOnce(final ATest test, final ITestContext ctx) throws Exception {
        synchronized (Ignite2ServerTestStub.class) {
            if (Ignite2ServerTestStub.lastNode == null) {
                try {
                    Ignite2ServerTestStub.lastNode = MergedContext.getInstance().getBean(ConfiguredIgnite2Server.class);
                } catch (final NoSuchBeanDefinitionException e) { //SUPPRESS CHECKSTYLE empty block
                    //ignore
                }
            }
        }
    }

    @Override
    public void tearDownOnce(final ATest test, final ITestContext ctx) throws Exception {
        if (!ctx.isFinishedGlobal()) {
            return;
        }
        maybeStopLastServer();
    }

    private static synchronized void maybeStopLastServer() throws Exception {
        if (lastNode != null) {
            lastNode.stop();
            lastNode = null;
        }
    }

}
