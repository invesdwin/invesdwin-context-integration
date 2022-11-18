package de.invesdwin.context.integration.jppf.node.test;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import jakarta.inject.Named;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.beans.init.locations.PositionedResource;
import de.invesdwin.context.integration.jppf.node.ConfiguredJPPFNode;
import de.invesdwin.context.integration.jppf.node.JPPFNodeContextLocation;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.context.test.stub.StubSupport;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;

@Named
@NotThreadSafe
public class JPPFNodeTestStub extends StubSupport {

    private static volatile ConfiguredJPPFNode lastNode;

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
        //if for some reason the tearDownOnce was not executed on the last test (maybe maven killed it?), then try to stop here aswell
        maybeStopLastServer();
        final JPPFNodeTest annotation = Reflections.getAnnotation(test, JPPFNodeTest.class);
        if (annotation != null) {
            if (annotation.value()) {
                locations.add(JPPFNodeContextLocation.CONTEXT_LOCATION);
            } else {
                locations.remove(JPPFNodeContextLocation.CONTEXT_LOCATION);
            }
        }
    }

    @Override
    public void setUpOnce(final ATest test, final TestContext ctx) throws Exception {
        try {
            JPPFNodeTestStub.lastNode = MergedContext.getInstance().getBean(ConfiguredJPPFNode.class);
        } catch (final NoSuchBeanDefinitionException e) { //SUPPRESS CHECKSTYLE empty block
            //ignore
        }
    }

    @Override
    public void tearDownOnce(final ATest test) throws Exception {
        maybeStopLastServer();
    }

    private static void maybeStopLastServer() throws Exception {
        if (lastNode != null) {
            lastNode.stop();
            lastNode = null;
        }
    }

}
