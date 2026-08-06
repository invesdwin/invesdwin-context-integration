package de.invesdwin.context.integration.grid.ignite3.server.test;

import java.util.List;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.grid.ignite3.server.ConfiguredIgnite3Server;
import de.invesdwin.context.integration.grid.ignite3.server.Ignite3ServerContextLocation;
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
public class Ignite3ServerTestStub extends StubSupport {

    @GuardedBy("this.class")
    private static ConfiguredIgnite3Server lastNode;

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
        final Ignite3ServerTest annotation = Reflections.getAnnotation(test, Ignite3ServerTest.class);
        if (annotation != null) {
            if (annotation.value()) {
                locations.add(Ignite3ServerContextLocation.SERVER_CONTEXT_LOCATION);
            } else {
                locations.remove(Ignite3ServerContextLocation.SERVER_CONTEXT_LOCATION);
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
        synchronized (Ignite3ServerTestStub.class) {
            if (Ignite3ServerTestStub.lastNode == null) {
                try {
                    Ignite3ServerTestStub.lastNode = MergedContext.getInstance().getBean(ConfiguredIgnite3Server.class);
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
