package de.invesdwin.context.integration.jppf.server.test.internal;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.jppf.node.JPPFNodeContextLocation;
import de.invesdwin.context.integration.jppf.node.test.JPPFNodeTestStub;
import de.invesdwin.context.integration.jppf.server.ConfiguredJPPFServer;
import de.invesdwin.context.integration.jppf.server.JPPFServerContextLocation;
import de.invesdwin.context.integration.jppf.server.test.JPPFServerTest;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.context.test.stub.StubSupport;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;

@Named
@NotThreadSafe
public class JPPFServerTestStub extends StubSupport {

    private static volatile ConfiguredJPPFServer lastServer;

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

        //if for some reason the tearDownOnce was not executed on the last test (maybe maven killed it?), then try to stop here aswell
        maybeStopLastServer();
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
    public void setUpContext(final ATest test, final TestContext ctx) throws Exception {
        ctx.deactivateBean(JPPFNodeTestStub.class);
    }

    @Override
    public void setUpOnce(final ATest test, final TestContext ctx) throws Exception {
        try {
            JPPFServerTestStub.lastServer = MergedContext.getInstance().getBean(ConfiguredJPPFServer.class);
        } catch (final NoSuchBeanDefinitionException e) { //SUPPRESS CHECKSTYLE empty block
            //ignore
        }
    }

    @Override
    public void tearDownOnce(final ATest test) throws Exception {
        maybeStopLastServer();
    }

    private static void maybeStopLastServer() throws Exception {
        if (lastServer != null) {
            lastServer.stop();
            lastServer = null;
        }
    }

}
