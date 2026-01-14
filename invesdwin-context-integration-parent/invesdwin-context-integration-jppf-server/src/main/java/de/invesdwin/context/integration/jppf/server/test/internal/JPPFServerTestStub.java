package de.invesdwin.context.integration.jppf.server.test.internal;

import java.util.List;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.jppf.node.JPPFNodeContextLocation;
import de.invesdwin.context.integration.jppf.node.test.JPPFNodeTestStub;
import de.invesdwin.context.integration.jppf.server.ConfiguredJPPFServer;
import de.invesdwin.context.integration.jppf.server.JPPFServerContextLocation;
import de.invesdwin.context.integration.jppf.server.test.JPPFServerTest;
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
public class JPPFServerTestStub extends StubSupport {

    @GuardedBy("this.class")
    private static ConfiguredJPPFServer lastServer;

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
        //invoke that stub before this one
        new JPPFNodeTestStub().setUpContextLocations(test, locations);

        final JPPFServerTest annotation = Reflections.getAnnotation(test, JPPFServerTest.class);
        if (annotation != null) {
            if (annotation.value()) {
                locations.add(JPPFServerContextLocation.CONTEXT_LOCATION);
                if (locations.contains(JPPFNodeContextLocation.CONTEXT_LOCATION)) {
                    ConfiguredJPPFServer.setNodeStartupEnabled(true);
                    locations.remove(JPPFNodeContextLocation.CONTEXT_LOCATION);
                }
            } else {
                locations.remove(JPPFServerContextLocation.CONTEXT_LOCATION);
            }
        }
    }

    @Override
    public void setUpContext(final ATest test, final ITestContextSetup ctx) throws Exception {
        ctx.deactivateBean(JPPFNodeTestStub.class);
        if (ctx.isPreMergedContext()) {
            return;
        }
        //if for some reason the tearDownOnce was not executed on the last test (maybe maven killed it?), then try to stop here aswell
        maybeStopLastServer();
    }

    @Override
    public void setUpOnce(final ATest test, final ITestContext ctx) throws Exception {
        synchronized (JPPFServerTestStub.class) {
            if (JPPFServerTestStub.lastServer == null) {
                try {
                    JPPFServerTestStub.lastServer = MergedContext.getInstance().getBean(ConfiguredJPPFServer.class);
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
        if (lastServer != null) {
            lastServer.stop();
            lastServer = null;
        }
    }

}
